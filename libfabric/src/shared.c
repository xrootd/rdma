#include <assert.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>

#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_atomic.h>

#include "shared.h"

struct fi_info *fi_pep, *fi, *hints;
struct fid_fabric* fabric;
struct fid_wait* waitset;
struct fid_domain* domain;
struct fid_poll* pollset;
struct fid_pep* pep;
struct fid_ep *ep, *alias_ep;
struct fid_cq *txcq, *rxcq;
struct fid_cntr *txcntr, *rxcntr;
struct fid_mr* mr;
void* mr_desc = NULL;
struct fid_av* av;
struct fid_eq* eq;
struct fid_mc* mc;

struct fid_mr no_mr;
struct fi_context tx_ctx, rx_ctx;
struct ft_context *tx_ctx_arr = NULL, *rx_ctx_arr = NULL;

uint64_t tx_seq, rx_seq, tx_cq_cntr, rx_cq_cntr;
int (*ft_mr_alloc_func)(void);
uint64_t ft_tag = 0;
pid_t ft_child_pid = 0;

fi_addr_t remote_fi_addr = FI_ADDR_UNSPEC;
char *buf, *tx_buf, *rx_buf;
char **tx_mr_bufs = NULL, **rx_mr_bufs = NULL;
size_t buf_size, tx_size, rx_size, tx_mr_size, rx_mr_size;
int rx_fd = -1, tx_fd = -1;
char default_port[8] = "9228";
static char default_oob_port[8] = "3000";

int timeout = -1;

int listen_sock = -1;
int oob_sock = -1;

struct fi_av_attr av_attr = {
  .type = FI_AV_MAP,
  .count = 1};
struct fi_eq_attr eq_attr = {
  .wait_obj = FI_WAIT_UNSPEC};
struct fi_cq_attr cq_attr = {
  .wait_obj = FI_WAIT_NONE};
struct fi_cntr_attr cntr_attr = {
  .events = FI_CNTR_EVENTS_COMP,
  .wait_obj = FI_WAIT_NONE};

struct fi_rma_iov remote;

struct ft_opts opts;

static const char integ_alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length = (sizeof(integ_alphabet) / sizeof(*integ_alphabet)) - 1;

static int ft_poll_fd(int fd, int timeout)
{
  struct pollfd fds;
  int ret;

  fds.fd = fd;
  fds.events = POLLIN;
  ret = poll(&fds, 1, timeout);
  if (ret == -1) {
    FT_PRINTERR("poll", -errno);
    ret = -errno;
  } else if (!ret) {
    ret = -FI_EAGAIN;
  } else {
    ret = 0;
  }
  return ret;
}

size_t ft_tx_prefix_size(void)
{
  return (fi->tx_attr->mode & FI_MSG_PREFIX) ? fi->ep_attr->msg_prefix_size : 0;
}

size_t ft_rx_prefix_size(void)
{
  return (fi->rx_attr->mode & FI_MSG_PREFIX) ? fi->ep_attr->msg_prefix_size : 0;
}

int ft_check_opts(uint64_t flags)
{
  return (opts.options & flags) == flags;
}

static void ft_cq_set_wait_attr(void)
{
  switch (opts.comp_method) {
    case FT_COMP_SREAD:
      cq_attr.wait_obj = FI_WAIT_UNSPEC;
      cq_attr.wait_cond = FI_CQ_COND_NONE;
      break;
    case FT_COMP_WAITSET:
      assert(waitset);
      cq_attr.wait_obj = FI_WAIT_SET;
      cq_attr.wait_cond = FI_CQ_COND_NONE;
      cq_attr.wait_set = waitset;
      break;
    case FT_COMP_WAIT_FD:
      cq_attr.wait_obj = FI_WAIT_FD;
      cq_attr.wait_cond = FI_CQ_COND_NONE;
      break;
    default:
      cq_attr.wait_obj = FI_WAIT_NONE;
      break;
  }
}

static void ft_cntr_set_wait_attr(void)
{
  switch (opts.comp_method) {
    case FT_COMP_SREAD:
      cntr_attr.wait_obj = FI_WAIT_UNSPEC;
      break;
    case FT_COMP_WAITSET:
      assert(waitset);
      cntr_attr.wait_obj = FI_WAIT_SET;
      break;
    case FT_COMP_WAIT_FD:
      cntr_attr.wait_obj = FI_WAIT_FD;
      break;
    default:
      cntr_attr.wait_obj = FI_WAIT_NONE;
      break;
  }
}

int ft_cntr_open(struct fid_cntr** cntr)
{
  ft_cntr_set_wait_attr();
  return fi_cntr_open(domain, &cntr_attr, cntr, cntr);
}

static inline int ft_rma_read_target_allowed(uint64_t caps)
{
  if (caps & (FI_RMA | FI_ATOMIC)) {
    if (caps & FI_REMOTE_READ)
      return 1;
    return !(caps & (FI_READ | FI_WRITE | FI_REMOTE_WRITE));
  }
  return 0;
}

static inline int ft_rma_write_target_allowed(uint64_t caps)
{
  if (caps & (FI_RMA | FI_ATOMIC)) {
    if (caps & FI_REMOTE_WRITE)
      return 1;
    return !(caps & (FI_READ | FI_WRITE | FI_REMOTE_WRITE));
  }
  return 0;
}

static inline int ft_check_mr_local_flag(struct fi_info* info)
{
  return ((info->mode & FI_LOCAL_MR) ||
          (info->domain_attr->mr_mode & FI_MR_LOCAL));
}

uint64_t ft_info_to_mr_access(struct fi_info* info)
{
  uint64_t mr_access = 0;
  if (ft_check_mr_local_flag(info)) {
    if (info->caps & (FI_MSG | FI_TAGGED)) {
      if (info->caps & FT_MSG_MR_ACCESS) {
        mr_access |= info->caps & FT_MSG_MR_ACCESS;
      } else {
        mr_access |= FT_MSG_MR_ACCESS;
      }
    }

    if (info->caps & (FI_RMA | FI_ATOMIC)) {
      if (info->caps & FT_RMA_MR_ACCESS) {
        mr_access |= info->caps & FT_RMA_MR_ACCESS;
      } else {
        mr_access |= FT_RMA_MR_ACCESS;
      }
    }
  } else {
    if (info->caps & (FI_RMA | FI_ATOMIC)) {
      if (ft_rma_read_target_allowed(info->caps)) {
        mr_access |= FI_REMOTE_READ;
      }
      if (ft_rma_write_target_allowed(info->caps)) {
        mr_access |= FI_REMOTE_WRITE;
      }
    }
  }
  return mr_access;
}

static int ft_reg_mr(void* buf, size_t size, uint64_t access,
                     uint64_t key, struct fid_mr** mr, void** desc)
{
  struct fi_mr_attr attr = {0};
  struct iovec iov = {0};
  int ret;

  if (((!(fi->domain_attr->mr_mode & FI_MR_LOCAL) && !(opts.options & FT_OPT_USE_DEVICE))) && !(fi->caps & (FI_RMA | FI_ATOMIC)))
    return 0;

  iov.iov_base = buf;
  iov.iov_len = size;
  attr.mr_iov = &iov;
  attr.iov_count = 1;
  attr.access = access;
  attr.offset = 0;
  attr.requested_key = key;
  attr.context = NULL;

  ret = fi_mr_regattr(domain, &attr, 0, mr);
  if (ret)
    return ret;

  if (desc)
    *desc = fi_mr_desc(*mr);

  return FI_SUCCESS;
}

static int ft_alloc_ctx_array(struct ft_context** mr_array, char*** mr_bufs,
                              char* default_buf, size_t mr_size,
                              uint64_t start_key)
{
  int i, ret;
  uint64_t access = ft_info_to_mr_access(fi);
  struct ft_context* context;

  *mr_array = calloc(opts.window_size, sizeof(**mr_array));
  if (!*mr_array)
    return -FI_ENOMEM;

  if (opts.options & FT_OPT_ALLOC_MULT_MR) {
    *mr_bufs = calloc(opts.window_size, sizeof(**mr_bufs));
    if (!mr_bufs)
      return -FI_ENOMEM;
  }

  for (i = 0; i < opts.window_size; i++) {
    context = &(*mr_array)[i];
    if (!(opts.options & FT_OPT_ALLOC_MULT_MR)) {
      context->buf = default_buf + mr_size * i;
      context->mr = mr;
      context->desc = mr_desc;
      continue;
    }
    ret = ft_hmem_alloc(opts.iface, opts.device,
                        (void**)&((*mr_bufs)[i]), mr_size);
    if (ret)
      return ret;

    context->buf = (*mr_bufs)[i];

    ret = ft_reg_mr(context->buf, mr_size, access,
                    start_key + i, &context->mr,
                    &context->desc);
    if (ret)
      return ret;
  }

  return 0;
}

static void ft_set_tx_rx_sizes(size_t* set_tx, size_t* set_rx)
{
  *set_tx = opts.transfer_size;
  if (*set_tx > fi->ep_attr->max_msg_size)
    *set_tx = fi->ep_attr->max_msg_size;
  *set_rx = *set_tx + ft_rx_prefix_size();
  *set_tx += ft_tx_prefix_size();
}

/*
 * Include FI_MSG_PREFIX space in the allocated buffer, and ensure that the
 * buffer is large enough for a control message used to exchange addressing
 * data.
 */
static int ft_alloc_msgs(void)
{
  int ret;
  long alignment = 1;

  if (ft_check_opts(FT_OPT_SKIP_MSG_ALLOC))
    return 0;

  if (opts.options & FT_OPT_ALLOC_MULT_MR) {
    ft_set_tx_rx_sizes(&tx_mr_size, &rx_mr_size);
    rx_size = FT_MAX_CTRL_MSG + ft_rx_prefix_size();
    tx_size = FT_MAX_CTRL_MSG + ft_tx_prefix_size();
    buf_size = rx_size + tx_size;
  } else {
    ft_set_tx_rx_sizes(&tx_size, &rx_size);
    tx_mr_size = 0;
    rx_mr_size = 0;
    buf_size = MAX(tx_size, FT_MAX_CTRL_MSG) * opts.window_size +
               MAX(rx_size, FT_MAX_CTRL_MSG) * opts.window_size;
  }

  if (opts.options & FT_OPT_ALIGN && !(opts.options & FT_OPT_USE_DEVICE)) {
    alignment = sysconf(_SC_PAGESIZE);
    if (alignment < 0)
      return -errno;
    buf_size += alignment;

    ret = posix_memalign((void**)&buf, (size_t)alignment,
                         buf_size);
    if (ret) {
      FT_PRINTERR("posix_memalign", ret);
      return ret;
    }
  } else {
    ret = ft_hmem_alloc(opts.iface, opts.device, (void**)&buf, buf_size);
    if (ret)
      return ret;
  }
  ret = ft_hmem_memset(opts.iface, opts.device, (void*)buf, 0, buf_size);
  if (ret)
    return ret;
  rx_buf = buf;

  if (opts.options & FT_OPT_ALLOC_MULT_MR)
    tx_buf = (char*)buf + MAX(rx_size, FT_MAX_CTRL_MSG);
  else
    tx_buf = (char*)buf + MAX(rx_size, FT_MAX_CTRL_MSG) * opts.window_size;

  ft_init_cq_data(fi);

  mr = &no_mr;
  if (!ft_mr_alloc_func && !ft_check_opts(FT_OPT_SKIP_REG_MR)) {
    ret = ft_reg_mr(buf, buf_size, ft_info_to_mr_access(fi),
                    FT_MR_KEY, &mr, &mr_desc);
    if (ret)
      return ret;
  } else {
    if (ft_mr_alloc_func) {
      assert(!ft_check_opts(FT_OPT_SKIP_REG_MR));
      ret = ft_mr_alloc_func();
      if (ret)
        return ret;
    }
  }

  ret = ft_alloc_ctx_array(&tx_ctx_arr, &tx_mr_bufs, tx_buf,
                           tx_mr_size, FT_TX_MR_KEY);
  if (ret)
    return -FI_ENOMEM;

  ret = ft_alloc_ctx_array(&rx_ctx_arr, &rx_mr_bufs, rx_buf,
                           rx_mr_size, FT_RX_MR_KEY);
  if (ret)
    return -FI_ENOMEM;

  return 0;
}

int ft_open_fabric_res(void)
{
  int ret;

  ret = fi_fabric(fi->fabric_attr, &fabric, NULL);
  if (ret) {
    FT_PRINTERR("fi_fabric", ret);
    return ret;
  }

  ret = fi_eq_open(fabric, &eq_attr, &eq, NULL);
  if (ret) {
    FT_PRINTERR("fi_eq_open", ret);
    return ret;
  }

  ret = fi_domain(fabric, fi, &domain, NULL);
  if (ret) {
    FT_PRINTERR("fi_domain", ret);
    return ret;
  }

  if (opts.options & FT_OPT_DOMAIN_EQ) {
    ret = fi_domain_bind(domain, &eq->fid, 0);
    if (ret) {
      FT_PRINTERR("fi_domain_bind", ret);
      return ret;
    }
  }

  return 0;
}

int ft_alloc_ep_res(struct fi_info* fi)
{
  int ret;

  ret = ft_alloc_msgs();
  if (ret)
    return ret;

  if (cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
    if (fi->caps & FI_TAGGED)
      cq_attr.format = FI_CQ_FORMAT_TAGGED;
    else
      cq_attr.format = FI_CQ_FORMAT_CONTEXT;
  }

  if (opts.options & FT_OPT_CQ_SHARED) {
    ft_cq_set_wait_attr();
    cq_attr.size = 0;

    if (opts.tx_cq_size)
      cq_attr.size += opts.tx_cq_size;
    else
      cq_attr.size += fi->tx_attr->size;

    if (opts.rx_cq_size)
      cq_attr.size += opts.rx_cq_size;
    else
      cq_attr.size += fi->rx_attr->size;

    ret = fi_cq_open(domain, &cq_attr, &txcq, &txcq);
    if (ret) {
      FT_PRINTERR("fi_cq_open", ret);
      return ret;
    }
    rxcq = txcq;
  }

  if (!(opts.options & FT_OPT_CQ_SHARED)) {
    ft_cq_set_wait_attr();
    if (opts.tx_cq_size)
      cq_attr.size = opts.tx_cq_size;
    else
      cq_attr.size = fi->tx_attr->size;

    ret = fi_cq_open(domain, &cq_attr, &txcq, &txcq);
    if (ret) {
      FT_PRINTERR("fi_cq_open", ret);
      return ret;
    }
  }

  if (opts.options & FT_OPT_TX_CNTR) {
    ret = ft_cntr_open(&txcntr);
    if (ret) {
      FT_PRINTERR("fi_cntr_open", ret);
      return ret;
    }
  }

  if (!(opts.options & FT_OPT_CQ_SHARED)) {
    ft_cq_set_wait_attr();
    if (opts.rx_cq_size)
      cq_attr.size = opts.rx_cq_size;
    else
      cq_attr.size = fi->rx_attr->size;

    ret = fi_cq_open(domain, &cq_attr, &rxcq, &rxcq);
    if (ret) {
      FT_PRINTERR("fi_cq_open", ret);
      return ret;
    }
  }

  if (opts.options & FT_OPT_RX_CNTR) {
    ret = ft_cntr_open(&rxcntr);
    if (ret) {
      FT_PRINTERR("fi_cntr_open", ret);
      return ret;
    }
  }

  if (fi->ep_attr->type == FI_EP_RDM || fi->ep_attr->type == FI_EP_DGRAM) {
    if (fi->domain_attr->av_type != FI_AV_UNSPEC)
      av_attr.type = fi->domain_attr->av_type;

    if (opts.av_name) {
      av_attr.name = opts.av_name;
    }
    av_attr.count = opts.av_size;
    ret = fi_av_open(domain, &av_attr, &av, NULL);
    if (ret) {
      FT_PRINTERR("fi_av_open", ret);
      return ret;
    }
  }
  return 0;
}

int ft_alloc_active_res(struct fi_info* fi)
{
  int ret;
  ret = ft_alloc_ep_res(fi);
  if (ret)
    return ret;

  ret = fi_endpoint(domain, fi, &ep, NULL);
  if (ret) {
    FT_PRINTERR("fi_endpoint", ret);
    return ret;
  }

  return 0;
}

static int ft_init(void)
{
  tx_seq = 0;
  rx_seq = 0;
  tx_cq_cntr = 0;
  rx_cq_cntr = 0;

  // If using device memory for transfers, require OOB address
  // exchange because extra steps are involved when passing
  // device buffers into fi_av_insert
  if (opts.options & FT_OPT_ENABLE_HMEM)
    opts.options |= FT_OPT_OOB_ADDR_EXCH;

  return ft_hmem_init(opts.iface);
}

int ft_sock_setup(int sock)
{
  int ret, op;

  op = 1;
  ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
                   (void*)&op, sizeof(op));
  if (ret)
    return ret;

  ret = ft_fd_nonblock(sock);
  if (ret)
    return ret;

  return 0;
}

int ft_init_oob(void)
{
  struct addrinfo* ai = NULL;
  int ret;

  if (!(opts.options & FT_OPT_OOB_CTRL) || oob_sock != -1)
    return 0;

  if (!opts.oob_port)
    opts.oob_port = default_oob_port;

  if (!opts.dst_addr) {
    ret = ft_sock_listen(opts.src_addr, opts.oob_port);
    if (ret)
      return ret;

    oob_sock = accept(listen_sock, NULL, 0);
    if (oob_sock < 0) {
      perror("accept");
      ret = oob_sock;
      return ret;
    }

    close(listen_sock);
  } else {
    ret = getaddrinfo(opts.dst_addr, opts.oob_port, NULL, &ai);
    if (ret) {
      perror("getaddrinfo");
      return ret;
    }

    oob_sock = socket(ai->ai_family, SOCK_STREAM, 0);
    if (oob_sock < 0) {
      perror("socket");
      ret = oob_sock;
      goto free;
    }

    ret = connect(oob_sock, ai->ai_addr, ai->ai_addrlen);
    if (ret) {
      perror("connect");
      close(oob_sock);
      goto free;
    }
    sleep(1);
  }

  ret = ft_sock_setup(oob_sock);

free:
  if (ai)
    freeaddrinfo(ai);
  return ret;
}

int ft_getinfo(struct fi_info* hints, struct fi_info** info)
{
  char *node, *service;
  uint64_t flags = 0;
  int ret;

  ret = ft_read_addr_opts(&node, &service, hints, &flags, &opts);
  if (ret)
    return ret;

  if (!hints->ep_attr->type)
    hints->ep_attr->type = FI_EP_RDM;

  ret = fi_getinfo(fi_version(), node, service, flags, hints, info);
  if (ret) {
    FT_PRINTERR("fi_getinfo", ret);
    return ret;
  }

  if (!ft_check_prefix_forced(*info, &opts)) {
    FT_ERR("Provider disabled requested prefix mode.");
    return -FI_ENODATA;
  }

  return 0;
}

int ft_init_fabric(void)
{
  int ret;

  ret = ft_init();
  if (ret)
    return ret;

  ret = ft_init_oob();
  if (ret)
    return ret;

  ret = ft_getinfo(hints, &fi);
  if (ret)
    return ret;

  ret = ft_open_fabric_res();
  if (ret)
    return ret;

  ret = ft_alloc_active_res(fi);
  if (ret)
    return ret;

  ret = ft_enable_ep_recv();
  if (ret)
    return ret;

  ret = ft_init_av();
  if (ret)
    return ret;

  if (ft_check_opts(FT_OPT_FORK_CHILD))
    ft_fork_child();

  return 0;
}

int ft_get_cq_fd(struct fid_cq* cq, int* fd)
{
  int ret = FI_SUCCESS;

  if (cq && opts.comp_method == FT_COMP_WAIT_FD) {
    ret = fi_control(&cq->fid, FI_GETWAIT, fd);
    if (ret)
      FT_PRINTERR("fi_control(FI_GETWAIT)", ret);
  }

  return ret;
}

int ft_enable_ep(struct fid_ep* ep, struct fid_eq* eq, struct fid_av* av,
                 struct fid_cq* txcq, struct fid_cq* rxcq,
                 struct fid_cntr* txcntr, struct fid_cntr* rxcntr)
{
  uint64_t flags;
  int ret;

  if ((fi->ep_attr->type == FI_EP_MSG || fi->caps & FI_MULTICAST) && !(opts.options & FT_OPT_DOMAIN_EQ))
    FT_EP_BIND(ep, eq, 0);

  FT_EP_BIND(ep, av, 0);

  flags = FI_TRANSMIT;
  if (!(opts.options & FT_OPT_TX_CQ))
    flags |= FI_SELECTIVE_COMPLETION;
  FT_EP_BIND(ep, txcq, flags);

  flags = FI_RECV;
  if (!(opts.options & FT_OPT_RX_CQ))
    flags |= FI_SELECTIVE_COMPLETION;
  FT_EP_BIND(ep, rxcq, flags);

  ret = ft_get_cq_fd(txcq, &tx_fd);
  if (ret)
    return ret;

  ret = ft_get_cq_fd(rxcq, &rx_fd);
  if (ret)
    return ret;

  /* TODO: use control structure to select counter bindings explicitly */
  if (opts.options & FT_OPT_TX_CQ)
    flags = 0;
  else
    flags = FI_SEND;
  if (hints->caps & (FI_WRITE | FI_READ))
    flags |= hints->caps & (FI_WRITE | FI_READ);
  else if (hints->caps & FI_RMA)
    flags |= FI_WRITE | FI_READ;
  FT_EP_BIND(ep, txcntr, flags);

  if (opts.options & FT_OPT_RX_CQ)
    flags = 0;
  else
    flags = FI_RECV;
  if (hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ))
    flags |= hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ);
  else if (hints->caps & FI_RMA)
    flags |= FI_REMOTE_WRITE | FI_REMOTE_READ;
  FT_EP_BIND(ep, rxcntr, flags);

  ret = fi_enable(ep);
  if (ret) {
    FT_PRINTERR("fi_enable", ret);
    return ret;
  }

  return 0;
}

int ft_enable_ep_recv(void)
{
  int ret;

  ret = ft_enable_ep(ep, eq, av, txcq, rxcq, txcntr, rxcntr);
  if (ret)
    return ret;

  if (!ft_check_opts(FT_OPT_SKIP_MSG_ALLOC) &&
      (fi->caps & (FI_MSG | FI_TAGGED))) {
    /* Initial receive will get remote address for unconnected EPs */
    ret = ft_post_rx(ep, MAX(rx_size, FT_MAX_CTRL_MSG), &rx_ctx);
    if (ret)
      return ret;
  }

  return 0;
}

int ft_av_insert(struct fid_av* av, void* addr, size_t count, fi_addr_t* fi_addr,
                 uint64_t flags, void* context)
{
  int ret;

  ret = fi_av_insert(av, addr, count, fi_addr, flags, context);
  if (ret < 0) {
    FT_PRINTERR("fi_av_insert", ret);
    return ret;
  } else if (ret != count) {
    FT_ERR(
      "fi_av_insert: number of addresses inserted = %d;"
      " number of addresses given = %zd\n",
      ret, count);
    return -EXIT_FAILURE;
  }

  return 0;
}

int ft_init_av(void)
{
  return ft_init_av_dst_addr(av, ep, &remote_fi_addr);
}

int ft_exchange_addresses_oob(struct fid_av* av_ptr, struct fid_ep* ep_ptr,
                              fi_addr_t* remote_addr)
{
  char buf[FT_MAX_CTRL_MSG];
  int ret;
  size_t addrlen = FT_MAX_CTRL_MSG;

  ret = fi_getname(&ep_ptr->fid, buf, &addrlen);
  if (ret) {
    FT_PRINTERR("fi_getname", ret);
    return ret;
  }

  ret = ft_sock_send(oob_sock, buf, FT_MAX_CTRL_MSG);
  if (ret)
    return ret;

  ret = ft_sock_recv(oob_sock, buf, FT_MAX_CTRL_MSG);
  if (ret)
    return ret;

  ret = ft_av_insert(av_ptr, buf, 1, remote_addr, 0, NULL);
  if (ret)
    return ret;

  return 0;
}

/* TODO: retry send for unreliable endpoints */
int ft_init_av_dst_addr(struct fid_av* av_ptr, struct fid_ep* ep_ptr,
                        fi_addr_t* remote_addr)
{
  size_t addrlen;
  int ret;

  if (opts.options & FT_OPT_OOB_ADDR_EXCH) {
    ret = ft_exchange_addresses_oob(av_ptr, ep_ptr, remote_addr);
    if (ret)
      return ret;
    else
      goto set_rx_seq_close;
  }

  if (opts.dst_addr) {
    ret = ft_av_insert(av_ptr, fi->dest_addr, 1, remote_addr, 0, NULL);
    if (ret)
      return ret;

    addrlen = FT_MAX_CTRL_MSG;
    ret = fi_getname(&ep_ptr->fid, (char*)tx_buf + ft_tx_prefix_size(),
                     &addrlen);
    if (ret) {
      FT_PRINTERR("fi_getname", ret);
      return ret;
    }

    ret = (int)ft_tx(ep, *remote_addr, addrlen, &tx_ctx);
    if (ret)
      return ret;

    ret = ft_rx(ep, 1);
    if (ret)
      return ret;
  } else {
    ret = ft_get_rx_comp(rx_seq);
    if (ret)
      return ret;

    /* Test passing NULL fi_addr on one of the sides (server) if
     * AV type is FI_AV_TABLE */
    ret = ft_av_insert(av_ptr, (char*)rx_buf + ft_rx_prefix_size(),
                       1, ((fi->domain_attr->av_type == FI_AV_TABLE) ? NULL : remote_addr), 0, NULL);
    if (ret)
      return ret;

    ret = ft_post_rx(ep, rx_size, &rx_ctx);
    if (ret)
      return ret;

    if (fi->domain_attr->av_type == FI_AV_TABLE)
      *remote_addr = 0;

    ret = (int)ft_tx(ep, *remote_addr, 1, &tx_ctx);
    if (ret)
      return ret;
  }

set_rx_seq_close:
  /*
   * For a test which does not have MSG or TAGGED
   * capabilities, but has RMA/Atomics and uses the OOB sync.
   * If no recv is going to be posted,
   * then the rx_seq needs to be incremented to wait on the first RMA/Atomic
   * completion.
   */
  if (!(fi->caps & FI_MSG) && !(fi->caps & FI_TAGGED) && opts.oob_port)
    rx_seq++;

  return 0;
}

int ft_exchange_raw_keys(struct fi_rma_iov* peer_iov)
{
  struct fi_rma_iov* rma_iov;
  size_t key_size;
  size_t len;
  uint64_t addr;
  int ret;

  /* Get key size */
  key_size = 0;
  ret = fi_mr_raw_attr(mr, &addr, NULL, &key_size, 0);
  if (ret != -FI_ETOOSMALL) {
    return ret;
  }

  len = sizeof(*rma_iov) + key_size - sizeof(rma_iov->key);
  /* TODO: make sure this fits in tx_buf and rx_buf */

  if (opts.dst_addr) {
    rma_iov = (struct fi_rma_iov*)(tx_buf + ft_tx_prefix_size());
    if ((fi->domain_attr->mr_mode == FI_MR_BASIC) ||
        (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
      rma_iov->addr = (uintptr_t)rx_buf + ft_rx_prefix_size();
    } else {
      rma_iov->addr = 0;
    }

    /* Get raw attributes */
    ret = fi_mr_raw_attr(mr, &addr, (uint8_t*)&rma_iov->key,
                         &key_size, 0);
    if (ret)
      return ret;

    ret = ft_tx(ep, remote_fi_addr, len, &tx_ctx);
    if (ret)
      return ret;

    ret = ft_get_rx_comp(rx_seq);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(rx_buf + ft_rx_prefix_size());
    peer_iov->addr = rma_iov->addr;
    peer_iov->len = rma_iov->len;
    /* Map remote mr raw locally */
    ret = fi_mr_map_raw(domain, rma_iov->addr,
                        (uint8_t*)&rma_iov->key, key_size,
                        &peer_iov->key, 0);
    if (ret)
      return ret;

    ret = ft_post_rx(ep, rx_size, &rx_ctx);
  } else {
    ret = ft_get_rx_comp(rx_seq);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(rx_buf + ft_rx_prefix_size());
    peer_iov->addr = rma_iov->addr;
    peer_iov->len = rma_iov->len;
    /* Map remote mr raw locally */
    ret = fi_mr_map_raw(domain, rma_iov->addr,
                        (uint8_t*)&rma_iov->key, key_size,
                        &peer_iov->key, 0);
    if (ret)
      return ret;

    ret = ft_post_rx(ep, rx_size, &rx_ctx);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(tx_buf + ft_tx_prefix_size());
    if ((fi->domain_attr->mr_mode == FI_MR_BASIC) ||
        (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
      rma_iov->addr = (uintptr_t)rx_buf + ft_rx_prefix_size();
    } else {
      rma_iov->addr = 0;
    }

    /* Get raw attributes */
    ret = fi_mr_raw_attr(mr, &addr, (uint8_t*)&rma_iov->key,
                         &key_size, 0);
    if (ret)
      return ret;

    ret = ft_tx(ep, remote_fi_addr, len, &tx_ctx);
  }

  return ret;
}

int ft_exchange_keys(struct fi_rma_iov* peer_iov)
{
  struct fi_rma_iov* rma_iov;
  int ret;

  if (fi->domain_attr->mr_mode & FI_MR_RAW)
    return ft_exchange_raw_keys(peer_iov);

  if (opts.dst_addr) {
    rma_iov = (struct fi_rma_iov*)(tx_buf + ft_tx_prefix_size());
    if ((fi->domain_attr->mr_mode == FI_MR_BASIC) ||
        (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
      rma_iov->addr = (uintptr_t)rx_buf + ft_rx_prefix_size();
    } else {
      rma_iov->addr = 0;
    }
    rma_iov->key = fi_mr_key(mr);
    ret = ft_tx(ep, remote_fi_addr, sizeof *rma_iov, &tx_ctx);
    if (ret)
      return ret;

    ret = ft_get_rx_comp(rx_seq);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(rx_buf + ft_rx_prefix_size());
    *peer_iov = *rma_iov;
    ret = ft_post_rx(ep, rx_size, &rx_ctx);
  } else {
    ret = ft_get_rx_comp(rx_seq);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(rx_buf + ft_rx_prefix_size());
    *peer_iov = *rma_iov;
    ret = ft_post_rx(ep, rx_size, &rx_ctx);
    if (ret)
      return ret;

    rma_iov = (struct fi_rma_iov*)(tx_buf + ft_tx_prefix_size());
    if ((fi->domain_attr->mr_mode == FI_MR_BASIC) ||
        (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
      rma_iov->addr = (uintptr_t)rx_buf + ft_rx_prefix_size();
    } else {
      rma_iov->addr = 0;
    }
    rma_iov->key = fi_mr_key(mr);
    ret = ft_tx(ep, remote_fi_addr, sizeof *rma_iov, &tx_ctx);
  }

  return ret;
}

static void ft_cleanup_mr_array(struct ft_context* ctx_arr, char** mr_bufs)
{
  int i, ret;

  if (!mr_bufs)
    return;

  for (i = 0; i < opts.window_size; i++) {
    FT_CLOSE_FID(ctx_arr[i].mr);
    ret = ft_hmem_free(opts.iface, mr_bufs[i]);
    if (ret)
      FT_PRINTERR("ft_hmem_free", ret);
  }
}

static void ft_close_fids(void)
{
  FT_CLOSE_FID(mc);
  FT_CLOSE_FID(alias_ep);
  FT_CLOSE_FID(ep);
  FT_CLOSE_FID(pep);
  if (opts.options & FT_OPT_CQ_SHARED) {
    FT_CLOSE_FID(txcq);
  } else {
    FT_CLOSE_FID(rxcq);
    FT_CLOSE_FID(txcq);
  }
  FT_CLOSE_FID(rxcntr);
  FT_CLOSE_FID(txcntr);
  FT_CLOSE_FID(pollset);
  if (mr != &no_mr)
    FT_CLOSE_FID(mr);
  FT_CLOSE_FID(av);
  FT_CLOSE_FID(domain);
  FT_CLOSE_FID(eq);
  FT_CLOSE_FID(waitset);
  FT_CLOSE_FID(fabric);
}

void ft_free_res(void)
{
  int ret;

  ft_cleanup_mr_array(tx_ctx_arr, tx_mr_bufs);
  ft_cleanup_mr_array(rx_ctx_arr, rx_mr_bufs);

  free(tx_ctx_arr);
  free(rx_ctx_arr);
  tx_ctx_arr = NULL;
  rx_ctx_arr = NULL;

  ft_close_fids();

  if (buf) {
    ret = ft_hmem_free(opts.iface, buf);
    if (ret)
      FT_PRINTERR("ft_hmem_free", ret);
    buf = rx_buf = tx_buf = NULL;
    buf_size = rx_size = tx_size = tx_mr_size = rx_mr_size = 0;
  }
  if (fi_pep) {
    fi_freeinfo(fi_pep);
    fi_pep = NULL;
  }
  if (fi) {
    fi_freeinfo(fi);
    fi = NULL;
  }
  if (hints) {
    fi_freeinfo(hints);
    hints = NULL;
  }

  ret = ft_hmem_cleanup(opts.iface);
  if (ret)
    FT_PRINTERR("ft_hmem_cleanup", ret);
}

static int dupaddr(void** dst_addr, size_t* dst_addrlen,
                   void* src_addr, size_t src_addrlen)
{
  *dst_addr = malloc(src_addrlen);
  if (!*dst_addr) {
    FT_ERR("address allocation failed");
    return EAI_MEMORY;
  }
  *dst_addrlen = src_addrlen;
  memcpy(*dst_addr, src_addr, src_addrlen);
  return 0;
}

static int getaddr(char* node, char* service,
                   struct fi_info* hints, uint64_t flags)
{
  int ret;
  struct fi_info* fi;

  if (!node && !service) {
    if (flags & FI_SOURCE) {
      hints->src_addr = NULL;
      hints->src_addrlen = 0;
    } else {
      hints->dest_addr = NULL;
      hints->dest_addrlen = 0;
    }
    return 0;
  }

  ret = fi_getinfo(FT_FIVERSION, node, service, flags, hints, &fi);
  if (ret) {
    FT_PRINTERR("fi_getinfo", ret);
    return ret;
  }
  hints->addr_format = fi->addr_format;

  if (flags & FI_SOURCE) {
    ret = dupaddr(&hints->src_addr, &hints->src_addrlen,
                  fi->src_addr, fi->src_addrlen);
  } else {
    ret = dupaddr(&hints->dest_addr, &hints->dest_addrlen,
                  fi->dest_addr, fi->dest_addrlen);
  }

  fi_freeinfo(fi);
  return ret;
}

int ft_getsrcaddr(char* node, char* service, struct fi_info* hints)
{
  return getaddr(node, service, hints, FI_SOURCE);
}

int ft_read_addr_opts(char** node, char** service, struct fi_info* hints,
                      uint64_t* flags, struct ft_opts* opts)
{
  int ret;

  if (opts->dst_addr && (opts->src_addr || !opts->oob_port)) {
    if (!opts->dst_port)
      opts->dst_port = default_port;

    ret = ft_getsrcaddr(opts->src_addr, opts->src_port, hints);
    if (ret)
      return ret;
    *node = opts->dst_addr;
    *service = opts->dst_port;
  } else {
    if (!opts->src_port)
      opts->src_port = default_port;

    *node = opts->src_addr;
    *service = opts->src_port;
    *flags = FI_SOURCE;
  }

  return 0;
}

static void ft_force_progress(void)
{
  if (txcq)
    fi_cq_read(txcq, NULL, 0);
  if (rxcq)
    fi_cq_read(rxcq, NULL, 0);
}

static int ft_progress(struct fid_cq* cq, uint64_t total, uint64_t* cq_cntr)
{
  struct fi_cq_err_entry comp;
  int ret;

  ret = fi_cq_read(cq, &comp, 1);
  if (ret > 0)
    (*cq_cntr)++;

  if (ret >= 0 || ret == -FI_EAGAIN)
    return 0;

  if (ret == -FI_EAVAIL) {
    ret = ft_cq_readerr(cq);
    (*cq_cntr)++;
  } else {
    FT_PRINTERR("fi_cq_read/sread", ret);
  }
  return ret;
}

#define FT_POST(post_fn, progress_fn, cq, seq, cq_cntr, op_str, ...) \
  do {                                                               \
    int timeout_save;                                                \
    int ret, rc;                                                     \
                                                                     \
    while (1) {                                                      \
      ret = post_fn(__VA_ARGS__);                                    \
      if (!ret)                                                      \
        break;                                                       \
                                                                     \
      if (ret != -FI_EAGAIN) {                                       \
        FT_PRINTERR(op_str, ret);                                    \
        return ret;                                                  \
      }                                                              \
                                                                     \
      timeout_save = timeout;                                        \
      timeout = 0;                                                   \
      rc = progress_fn(cq, seq, cq_cntr);                            \
      if (rc && rc != -FI_EAGAIN) {                                  \
        FT_ERR("Failed to get " op_str " completion");               \
        return rc;                                                   \
      }                                                              \
      timeout = timeout_save;                                        \
    }                                                                \
    seq++;                                                           \
  } while (0)

ssize_t ft_post_tx_buf(struct fid_ep* ep, fi_addr_t fi_addr, size_t size,
                       uint64_t data, void* ctx,
                       void* op_buf, void* op_mr_desc, uint64_t op_tag)
{
  size += ft_tx_prefix_size();
  if (hints->caps & FI_TAGGED) {
    op_tag = op_tag ? op_tag : tx_seq;
    if (data != NO_CQ_DATA) {
      FT_POST(fi_tsenddata, ft_progress, txcq, tx_seq,
              &tx_cq_cntr, "transmit", ep, op_buf, size,
              op_mr_desc, data, fi_addr, op_tag, ctx);
    } else {
      FT_POST(fi_tsend, ft_progress, txcq, tx_seq,
              &tx_cq_cntr, "transmit", ep, op_buf, size,
              op_mr_desc, fi_addr, op_tag, ctx);
    }
  } else {
    if (data != NO_CQ_DATA) {
      FT_POST(fi_senddata, ft_progress, txcq, tx_seq,
              &tx_cq_cntr, "transmit", ep, op_buf, size,
              op_mr_desc, data, fi_addr, ctx);

    } else {
      FT_POST(fi_send, ft_progress, txcq, tx_seq,
              &tx_cq_cntr, "transmit", ep, op_buf, size,
              op_mr_desc, fi_addr, ctx);
    }
  }
  return 0;
}

ssize_t ft_post_tx(struct fid_ep* ep, fi_addr_t fi_addr, size_t size,
                   uint64_t data, void* ctx)
{
  return ft_post_tx_buf(ep, fi_addr, size, data,
                        ctx, tx_buf, mr_desc, ft_tag);
}

ssize_t ft_tx(struct fid_ep* ep, fi_addr_t fi_addr, size_t size, void* ctx)
{
  ssize_t ret;

  if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
    ret = ft_fill_buf((char*)tx_buf + ft_tx_prefix_size(), size);
    if (ret)
      return ret;
  }

  ret = ft_post_tx(ep, fi_addr, size, NO_CQ_DATA, ctx);
  if (ret)
    return ret;

  ret = ft_get_tx_comp(tx_seq);
  return ret;
}

ssize_t ft_post_rx_buf(struct fid_ep* ep, size_t size, void* ctx,
                       void* op_buf, void* op_mr_desc, uint64_t op_tag)
{
  size = MAX(size, FT_MAX_CTRL_MSG) + ft_rx_prefix_size();
  if (hints->caps & FI_TAGGED) {
    op_tag = op_tag ? op_tag : rx_seq;
    FT_POST(fi_trecv, ft_progress, rxcq, rx_seq, &rx_cq_cntr,
            "receive", ep, op_buf, size, op_mr_desc,
            remote_fi_addr, op_tag, 0, ctx);
  } else {
    FT_POST(fi_recv, ft_progress, rxcq, rx_seq, &rx_cq_cntr,
            "receive", ep, op_buf, size, op_mr_desc, 0, ctx);
  }
  return 0;
}

ssize_t ft_post_rx(struct fid_ep* ep, size_t size, void* ctx)
{
  return ft_post_rx_buf(ep, size, ctx, rx_buf, mr_desc, ft_tag);
}

ssize_t ft_rx(struct fid_ep* ep, size_t size)
{
  ssize_t ret;

  ret = ft_get_rx_comp(rx_seq);
  if (ret)
    return ret;

  if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
    ret = ft_check_buf((char*)rx_buf + ft_rx_prefix_size(), size);
    if (ret)
      return ret;
  }
  /* TODO: verify CQ data, if available */

  /* Ignore the size arg. Post a buffer large enough to handle all message
   * sizes. ft_sync() makes use of ft_rx() and gets called in tests just before
   * message size is updated. The recvs posted are always for the next incoming
   * message */
  ret = ft_post_rx(ep, rx_size, &rx_ctx);
  return ret;
}

/*
 * Received messages match tagged buffers in order, but the completions can be
 * reported out of order.  A tag is valid if it's within the current window.
 */
static inline int
  ft_tag_is_valid(struct fid_cq* cq, struct fi_cq_err_entry* comp, uint64_t tag)
{
  int valid = 1;

  if ((hints->caps & FI_TAGGED) && (cq == rxcq)) {
    if (opts.options & FT_OPT_BW) {
      /* valid: (tag - window) < comp->tag < (tag + window) */
      valid = (tag < comp->tag + opts.window_size) &&
              (comp->tag < tag + opts.window_size);
    } else {
      valid = (comp->tag == tag);
    }

    if (!valid) {
      FT_ERR("Tag mismatch!. Expected: %" PRIu64 ", actual: %" PRIu64, tag, comp->tag);
    }
  }

  return valid;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_spin_for_comp(struct fid_cq* cq, uint64_t* cur,
                            uint64_t total, int timeout)
{
  struct fi_cq_err_entry comp;
  struct timespec a, b;
  int ret;

  if (timeout >= 0)
    clock_gettime(CLOCK_MONOTONIC, &a);

  do {
    ret = fi_cq_read(cq, &comp, 1);
    if (ret > 0) {
      if (timeout >= 0)
        clock_gettime(CLOCK_MONOTONIC, &a);
      if (!ft_tag_is_valid(cq, &comp, ft_tag ? ft_tag : rx_cq_cntr))
        return -FI_EOTHER;
      (*cur)++;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return ret;
    } else if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &b);
      if ((b.tv_sec - a.tv_sec) > timeout) {
        fprintf(stderr, "%ds timeout expired\n", timeout);
        return -FI_ENODATA;
      }
    }
  } while (total - *cur > 0);

  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_wait_for_comp(struct fid_cq* cq, uint64_t* cur,
                            uint64_t total, int timeout)
{
  struct fi_cq_err_entry comp;
  int ret;

  while (total - *cur > 0) {
    ret = fi_cq_sread(cq, &comp, 1, NULL, timeout);
    if (ret > 0) {
      if (!ft_tag_is_valid(cq, &comp, ft_tag ? ft_tag : rx_cq_cntr))
        return -FI_EOTHER;
      (*cur)++;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return ret;
    }
  }

  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_fdwait_for_comp(struct fid_cq* cq, uint64_t* cur,
                              uint64_t total, int timeout)
{
  struct fi_cq_err_entry comp;
  struct fid* fids[1];
  int fd, ret;

  fd = cq == txcq ? tx_fd : rx_fd;
  fids[0] = &cq->fid;

  while (total - *cur > 0) {
    ret = fi_trywait(fabric, fids, 1);
    if (ret == FI_SUCCESS) {
      ret = ft_poll_fd(fd, timeout);
      if (ret && ret != -FI_EAGAIN)
        return ret;
    }

    ret = fi_cq_read(cq, &comp, 1);
    if (ret > 0) {
      if (!ft_tag_is_valid(cq, &comp, ft_tag ? ft_tag : rx_cq_cntr))
        return -FI_EOTHER;
      (*cur)++;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return ret;
    }
  }

  return 0;
}

static int ft_get_cq_comp(struct fid_cq* cq, uint64_t* cur,
                          uint64_t total, int timeout)
{
  int ret;

  switch (opts.comp_method) {
    case FT_COMP_SREAD:
    case FT_COMP_YIELD:
      ret = ft_wait_for_comp(cq, cur, total, timeout);
      break;
    case FT_COMP_WAIT_FD:
      ret = ft_fdwait_for_comp(cq, cur, total, timeout);
      break;
    default:
      ret = ft_spin_for_comp(cq, cur, total, timeout);
      break;
  }

  if (ret) {
    if (ret == -FI_EAVAIL) {
      ret = ft_cq_readerr(cq);
      (*cur)++;
    } else {
      FT_PRINTERR("ft_get_cq_comp", ret);
    }
  }
  return ret;
}

static int ft_spin_for_cntr(struct fid_cntr* cntr, uint64_t total, int timeout)
{
  struct timespec a, b;
  uint64_t cur;

  if (timeout >= 0)
    clock_gettime(CLOCK_MONOTONIC, &a);

  for (;;) {
    cur = fi_cntr_read(cntr);
    if (cur >= total)
      return 0;

    if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &b);
      if ((b.tv_sec - a.tv_sec) > timeout)
        break;
    }
  }

  fprintf(stderr, "%ds timeout expired\n", timeout);
  return -FI_ENODATA;
}

static int ft_wait_for_cntr(struct fid_cntr* cntr, uint64_t total, int timeout)
{
  int ret;

  while (fi_cntr_read(cntr) < total) {
    ret = fi_cntr_wait(cntr, total, timeout);
    if (ret)
      FT_PRINTERR("fi_cntr_wait", ret);
    else
      break;
  }
  return 0;
}

static int ft_get_cntr_comp(struct fid_cntr* cntr, uint64_t total, int timeout)
{
  int ret = 0;

  switch (opts.comp_method) {
    case FT_COMP_SREAD:
    case FT_COMP_WAITSET:
    case FT_COMP_WAIT_FD:
    case FT_COMP_YIELD:
      ret = ft_wait_for_cntr(cntr, total, timeout);
      break;
    default:
      ret = ft_spin_for_cntr(cntr, total, timeout);
      break;
  }

  if (ret)
    FT_PRINTERR("fs_get_cntr_comp", ret);

  return ret;
}

int ft_get_rx_comp(uint64_t total)
{
  int ret = FI_SUCCESS;

  if (opts.options & FT_OPT_RX_CQ) {
    ret = ft_get_cq_comp(rxcq, &rx_cq_cntr, total, timeout);
  } else if (rxcntr) {
    ret = ft_get_cntr_comp(rxcntr, total, timeout);
  } else {
    FT_ERR("Trying to get a RX completion when no RX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return ret;
}

int ft_get_tx_comp(uint64_t total)
{
  int ret;

  if (opts.options & FT_OPT_TX_CQ) {
    ret = ft_get_cq_comp(txcq, &tx_cq_cntr, total, timeout);
  } else if (txcntr) {
    ret = ft_get_cntr_comp(txcntr, total, timeout); // -1);
  } else {
    FT_ERR("Trying to get a TX completion when no TX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return ret;
}

int ft_cq_readerr(struct fid_cq* cq)
{
  struct fi_cq_err_entry cq_err;
  int ret;

  memset(&cq_err, 0, sizeof(cq_err));
  ret = fi_cq_readerr(cq, &cq_err, 0);
  if (ret < 0) {
    FT_PRINTERR("fi_cq_readerr", ret);
  } else {
    FT_CQ_ERR(cq, cq_err, NULL, 0);
    ret = -cq_err.err;
  }
  return ret;
}

void eq_readerr(struct fid_eq* eq, const char* eq_str)
{
  struct fi_eq_err_entry eq_err;
  int rd;

  memset(&eq_err, 0, sizeof(eq_err));
  rd = fi_eq_readerr(eq, &eq_err, 0);
  if (rd != sizeof(eq_err)) {
    FT_PRINTERR("fi_eq_readerr", rd);
  } else {
    FT_EQ_ERR(eq, eq_err, NULL, 0);
  }
}

int ft_fork_child(void)
{
  ft_child_pid = fork();
  if (ft_child_pid < 0) {
    FT_PRINTERR("fork", ft_child_pid);
    return -errno;
  }

  if (ft_child_pid == 0) {
    exit(0);
  }

  return 0;
}

void ft_addr_usage()
{
  FT_PRINT_OPTS_USAGE("-B <src_port>", "non default source port number");
  FT_PRINT_OPTS_USAGE("-P <dst_port>", "non default destination port number");
  FT_PRINT_OPTS_USAGE("-s <address>", "source address");
  FT_PRINT_OPTS_USAGE("-b[=<oob_port>]",
                      "enable out-of-band address exchange and "
                      "synchronization over the, optional, port");
  FT_PRINT_OPTS_USAGE("-E[=<oob_port>]",
                      "enable out-of-band address exchange only "
                      "over the, optional, port");
  FT_PRINT_OPTS_USAGE("-C <number>",
                      "number of connections to accept before "
                      "cleaning up a server");
  FT_PRINT_OPTS_USAGE("-F <addr_format>", "Address format (default:FI_FORMAT_UNSPEC)");
}

void ft_usage(char* name, char* desc)
{
  fprintf(stderr, "Usage:\n");
  fprintf(stderr, "  %s [OPTIONS]\t\tstart server\n", name);
  fprintf(stderr, "  %s [OPTIONS] <host>\tconnect to server\n", name);

  if (desc)
    fprintf(stderr, "\n%s\n", desc);

  fprintf(stderr, "\nOptions:\n");
  ft_addr_usage();
  FT_PRINT_OPTS_USAGE("-f <fabric>", "fabric name");
  FT_PRINT_OPTS_USAGE("-d <domain>", "domain name");
  FT_PRINT_OPTS_USAGE("-p <provider>", "specific provider name eg sockets, verbs");
  FT_PRINT_OPTS_USAGE("-e <ep_type>", "Endpoint type: msg|rdm|dgram (default:rdm)");
  FT_PRINT_OPTS_USAGE("", "Only the following tests support this option for now:");
  FT_PRINT_OPTS_USAGE("", "fi_rma_bw");
  FT_PRINT_OPTS_USAGE("", "fi_shared_ctx");
  FT_PRINT_OPTS_USAGE("", "fi_multi_mr");
  FT_PRINT_OPTS_USAGE("", "fi_multi_ep");
  FT_PRINT_OPTS_USAGE("", "fi_recv_cancel");
  FT_PRINT_OPTS_USAGE("", "fi_unexpected_msg");
  FT_PRINT_OPTS_USAGE("", "fi_resmgmt_test");
  FT_PRINT_OPTS_USAGE("", "fi_inj_complete");
  FT_PRINT_OPTS_USAGE("", "fi_bw");
  FT_PRINT_OPTS_USAGE("-U", "run fabtests with FI_DELIVERY_COMPLETE set");
  FT_PRINT_OPTS_USAGE("-M <mode>", "Disable mode bit from test");
  FT_PRINT_OPTS_USAGE("-K", "fork a child process after initializing endpoint");
  FT_PRINT_OPTS_USAGE("", "mr_local");
  FT_PRINT_OPTS_USAGE("-a <address vector name>", "name of address vector");
  FT_PRINT_OPTS_USAGE("-h", "display this help output");

  return;
}

void ft_parseinfo(int op, char* optarg, struct fi_info* hints,
                  struct ft_opts* opts)
{
  switch (op) {
    case 'f':
      if (!hints->fabric_attr) {
        hints->fabric_attr = malloc(sizeof *(hints->fabric_attr));
        if (!hints->fabric_attr) {
          perror("malloc");
          exit(EXIT_FAILURE);
        }
      }
      hints->fabric_attr->name = strdup(optarg);
      break;
    case 'd':
      if (!hints->domain_attr) {
        hints->domain_attr = malloc(sizeof *(hints->domain_attr));
        if (!hints->domain_attr) {
          perror("malloc");
          exit(EXIT_FAILURE);
        }
      }
      hints->domain_attr->name = strdup(optarg);
      break;
    case 'p':
      if (!hints->fabric_attr) {
        hints->fabric_attr = malloc(sizeof *(hints->fabric_attr));
        if (!hints->fabric_attr) {
          perror("malloc");
          exit(EXIT_FAILURE);
        }
      }
      hints->fabric_attr->prov_name = strdup(optarg);
      break;
    case 'e':
      if (!strncasecmp("msg", optarg, 3))
        hints->ep_attr->type = FI_EP_MSG;
      if (!strncasecmp("rdm", optarg, 3))
        hints->ep_attr->type = FI_EP_RDM;
      if (!strncasecmp("dgram", optarg, 5))
        hints->ep_attr->type = FI_EP_DGRAM;
      break;
    case 'M':
      if (!strncasecmp("mr_local", optarg, 8))
        opts->mr_mode &= ~FI_MR_LOCAL;
      break;
    case 'D':
      if (!strncasecmp("ze", optarg, 2))
        opts->iface = FI_HMEM_ZE;
      else if (!strncasecmp("cuda", optarg, 4))
        opts->iface = FI_HMEM_CUDA;
      else
        printf("Unsupported interface\n");
      opts->options |= FT_OPT_ENABLE_HMEM | FT_OPT_USE_DEVICE;
      break;
    case 'i':
      opts->device = atoi(optarg);
      break;
    case 'H':
      opts->options |= FT_OPT_ENABLE_HMEM;
      break;
    case 'K':
      opts->options |= FT_OPT_FORK_CHILD;
      break;
    // parsing filenames
    case 'r':
      opts->src_filename = optarg;
      break;
    case 'y':
      opts->dst_filename = optarg;
      break;
    case 'v':
      opts->transfer_size = atol(optarg);
      break;
    default:
      /* let getopt handle unknown opts*/
      break;
  }
}

void ft_parse_addr_opts(int op, char* optarg, struct ft_opts* opts)
{
  switch (op) {
    case 's':
      opts->src_addr = optarg;
      break;
    case 'B':
      opts->src_port = optarg;
      break;
    case 'P':
      opts->dst_port = optarg;
      break;
    case 'b':
      opts->options |= FT_OPT_OOB_SYNC;
      /* fall through */
    case 'E':
      opts->options |= FT_OPT_OOB_ADDR_EXCH;
      if (optarg && strlen(optarg) > 1)
        opts->oob_port = optarg + 1;
      else
        opts->oob_port = default_oob_port;
      break;
    case 'F':
      if (!strncasecmp("fi_sockaddr_in", optarg, 14))
        opts->address_format = FI_SOCKADDR_IN;
      else if (!strncasecmp("fi_sockaddr_in6", optarg, 15))
        opts->address_format = FI_SOCKADDR_IN6;
      else if (!strncasecmp("fi_sockaddr_ib", optarg, 14))
        opts->address_format = FI_SOCKADDR_IB;
      else if (!strncasecmp("fi_sockaddr", optarg, 11)) /* keep me last */
        opts->address_format = FI_SOCKADDR;
      break;
    case 'C':
      opts->options |= FT_OPT_SERVER_PERSIST;
      opts->num_connections = atoi(optarg);
    default:
      /* let getopt handle unknown opts*/
      break;
  }
}

int ft_fill_buf(void* buf, size_t size)
{
  char* msg_buf;
  int msg_index = 0;
  size_t i;
  int ret = 0;

  if (opts.iface != FI_HMEM_SYSTEM) {
    msg_buf = malloc(size);
    if (!msg_buf)
      return -FI_ENOMEM;
  } else {
    msg_buf = (char*)buf;
  }

  for (i = 0; i < size; i++) {
    msg_buf[i] = integ_alphabet[msg_index];
    if (++msg_index >= integ_alphabet_length)
      msg_index = 0;
  }

  if (opts.iface != FI_HMEM_SYSTEM) {
    ret = ft_hmem_copy_to(opts.iface, opts.device, buf, msg_buf, size);
    if (ret)
      goto out;
  }
out:
  if (opts.iface != FI_HMEM_SYSTEM)
    free(msg_buf);
  return ret;
}

int ft_check_buf(void* buf, size_t size)
{
  char* recv_data;
  char c;
  int msg_index = 0;
  size_t i;
  int ret = 0;

  if (opts.iface != FI_HMEM_SYSTEM) {
    recv_data = malloc(size);
    if (!recv_data)
      return -FI_ENOMEM;

    ret = ft_hmem_copy_from(opts.iface, opts.device,
                            recv_data, buf, size);
    if (ret)
      goto out;
  } else {
    recv_data = (char*)buf;
  }

  for (i = 0; i < size; i++) {
    c = integ_alphabet[msg_index];
    if (++msg_index >= integ_alphabet_length)
      msg_index = 0;
    if (c != recv_data[i])
      break;
  }
  if (i != size) {
    printf(
      "Data check error (%c!=%c) at byte %zu for "
      "buffer size %zu\n",
      c, recv_data[i], i, size);
    ret = -FI_EIO;
  }

out:
  if (opts.iface != FI_HMEM_SYSTEM)
    free(recv_data);
  return ret;
}

uint64_t ft_init_cq_data(struct fi_info* info)
{
  if (info->domain_attr->cq_data_size >= sizeof(uint64_t)) {
    return 0x0123456789abcdefULL;
  } else {
    return 0x0123456789abcdef &
           ((0x1ULL << (info->domain_attr->cq_data_size * 8)) - 1);
  }
}

int ft_sock_listen(char* node, char* service)
{
  struct addrinfo *ai, hints;
  int val, ret;

  memset(&hints, 0, sizeof hints);
  hints.ai_flags = AI_PASSIVE;

  ret = getaddrinfo(node, service, &hints, &ai);
  if (ret) {
    fprintf(stderr, "getaddrinfo() %i\n", gai_strerror(ret));
    return ret;
  }

  listen_sock = socket(ai->ai_family, SOCK_STREAM, 0);
  if (listen_sock < 0) {
    perror("socket");
    ret = listen_sock;
    goto out;
  }

  val = 1;
  ret = setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR,
                   (void*)&val, sizeof val);
  if (ret) {
    perror("setsockopt SO_REUSEADDR");
    goto out;
  }

  ret = bind(listen_sock, ai->ai_addr, ai->ai_addrlen);
  if (ret) {
    perror("bind");
    goto out;
  }

  ret = listen(listen_sock, 0);
  if (ret)
    perror("listen");

out:
  if (ret && listen_sock >= 0)
    close(listen_sock);
  freeaddrinfo(ai);
  return ret;
}

int ft_sock_send(int fd, void* msg, size_t len)
{
  size_t sent;
  ssize_t ret, err = 0;

  for (sent = 0; sent < len;) {
    ret = send(fd, ((char*)msg) + sent, len - sent, 0);
    if (ret > 0) {
      sent += ret;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      ft_force_progress();
    } else {
      err = -errno;
      break;
    }
  }

  return err ? err : 0;
}

int ft_sock_recv(int fd, void* msg, size_t len)
{
  size_t rcvd;
  ssize_t ret, err = 0;

  for (rcvd = 0; rcvd < len;) {
    ret = recv(fd, ((char*)msg) + rcvd, len - rcvd, 0);
    if (ret > 0) {
      rcvd += ret;
    } else if (ret == 0) {
      err = -FI_ENOTCONN;
      break;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      ft_force_progress();
    } else {
      err = -errno;
      break;
    }
  }

  return err ? err : 0;
}
