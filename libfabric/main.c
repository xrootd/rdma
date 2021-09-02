#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>

#include <rdma/fi_errno.h>

#include "shared.h"

struct fi_rma_iov local, remote;
struct fi_context fi_ctx_write;
struct fi_context fi_ctx_read;

size_t chunk_size = 1024 * 1024;

static int run_test(void)
{
  int ret = 0;

  ret = ft_init_fabric();
  if (ret)
    return ret;

  ret = ft_exchange_keys(&remote);
  if (ret)
    return ret;

  if (opts.dst_addr) {
    // trying to read input file
    // ------------------------------------------
    fprintf(stdout, "Trying to read a file: %s\n", opts.src_filename);

    errno = 0;
    FILE* file_ptr = fopen(opts.src_filename, "rb");

    if (file_ptr == NULL) {
      fprintf(stdout, "The file does not exist! errno = %d\n", errno);
      return -1;
    }

    fseek(file_ptr, 0, SEEK_END);
    size_t file_len = ftell(file_ptr);

    printf("file_len = %ld\n", file_len);

    // sending output filename
    // ------------------------------------------
    if (snprintf(tx_buf, tx_size, "%s", opts.dst_filename) >= tx_size) {
      fprintf(stderr, "Transmit buffer too small.\n");
      return -FI_ETOOSMALL;
    }

    int name_len = strlen(opts.dst_filename) + 1;
    ret = fi_write(ep, tx_buf, name_len, mr_desc,
                   remote_fi_addr, remote.addr, remote.key,
                   &fi_ctx_write);
    if (ret)
      return ret;

    ret = ft_get_tx_comp(++tx_seq);
    if (ret)
      return ret;

    // sending file size to server
    // ------------------------------------------
    char file_len_ch[256];
    snprintf(file_len_ch, sizeof(file_len_ch), "%zu", file_len);
    snprintf(tx_buf, tx_size, "%s", file_len_ch);

    ret = fi_write(ep, tx_buf, sizeof(file_len_ch), mr_desc,
                   remote_fi_addr, remote.addr, remote.key,
                   &fi_ctx_write);
    if (ret)
      return ret;

    ret = ft_get_tx_comp(++tx_seq);
    if (ret)
      return ret;

    // sending a file to server
    // ------------------------------------------
    rewind(file_ptr);

    tx_buf = (char*)malloc((chunk_size + 1) * sizeof(char)); // allocating chunk
    size_t n_transx = file_len / chunk_size;                 // number of transactions

    if (file_len % chunk_size != 0)
      n_transx++;

    for (size_t itx = 0; itx < n_transx; itx++) {
      // clean tx buffer
      for (size_t i = 0; i < chunk_size; i++)
        tx_buf[i] = '\0';

      fread(tx_buf, chunk_size, 1, file_ptr);

      printf("RMA write to server, chunk #%ld\n", itx);

      ret = fi_write(ep, tx_buf, chunk_size, mr_desc,
                     remote_fi_addr, remote.addr, remote.key,
                     &fi_ctx_write);
      if (ret)
        return ret;

      ret = ft_get_tx_comp(tx_seq++);
      if (ret)
        return ret;
    }

    fclose(file_ptr);

    // waiting for finalization message
    // ------------------------------------------
    ret = ft_get_rx_comp(rx_seq++);
    if (ret)
      return ret;

    fprintf(stdout, "Received data from Server: %s\n", (char*)rx_buf);
  } else {
    // receiving destination filename and size
    // ------------------------------------------
    ret = ft_get_rx_comp(rx_seq++);
    if (ret)
      return ret;

    fprintf(stdout, "Received data from Client: %s\n", (char*)rx_buf);

    opts.dst_filename = (char*)rx_buf;

    fprintf(stdout, "Opening file for writing: %s\n", opts.dst_filename);
    FILE* dst_file_ptr = fopen(opts.dst_filename, "wb");

    size_t file_len;

    // clean recv buffer
    for (size_t i = 0; i < rx_size; i++)
      rx_buf[i] = '\0';

    ret = ft_get_rx_comp(rx_seq++);
    if (ret)
      return ret;

    file_len = atol(rx_buf);

    printf("file_len = %ld\n", file_len);

    // receiving data
    // ------------------------------------------

    size_t n_transx = file_len / chunk_size;

    if (file_len % chunk_size != 0)
      n_transx++;

    for (size_t irx = 0; irx < n_transx; irx++) {
      // clean recv buffer
      for (size_t i = 0; i < chunk_size; i++)
        rx_buf[i] = '\0';

      ret = ft_get_rx_comp(rx_seq++);
      if (ret)
        return ret;

      // fprintf(stdout, "Received data from Client: %s\n", (char *) rx_buf);
      fprintf(stdout, "Received data from Client: #%ld\n", irx);

      errno = 0;
      fwrite((char*)rx_buf, chunk_size, 1, dst_file_ptr);
    }

    fclose(dst_file_ptr);

    // sending a finalization message
    // ------------------------------------------
    char* fin_message = "Success: end of transmission";
    size_t msg_size = strlen(fin_message);
    if (snprintf(tx_buf, tx_size, "%s", fin_message) >= tx_size) {
      fprintf(stderr, "Transmit buffer too small.\n");
      return -FI_ETOOSMALL;
    }

    ret = fi_write(ep, tx_buf, msg_size, mr_desc,
                   remote_fi_addr, remote.addr, remote.key,
                   &fi_ctx_write);
    if (ret)
      return ret;

    ret = ft_get_tx_comp(tx_seq++);
    if (ret)
      return ret;
  }

  return 0;
}

int main(int argc, char** argv)
{
  setbuf(stdout, NULL);

  int op, ret;

  opts = INIT_OPTS;
  opts.options = FT_OPT_SIZE | FT_OPT_RX_CNTR | FT_OPT_TX_CNTR;

  // set timeout for get tx/rx operations
  timeout = 30;

  while ((op = getopt(argc, argv, "h" ADDR_OPTS INFO_OPTS FILE_OPTS)) != -1) {
    switch (op) {
      default:
        ft_parse_addr_opts(op, optarg, &opts);
        ft_parseinfo(op, optarg, hints, &opts);
        break;
      case '?':
      case 'h':
        ft_usage(argv[0], "A simple RDM client-server RMA example.");
        return EXIT_FAILURE;
    }
  }

  hints = fi_allocinfo();
  if (!hints)
    return EXIT_FAILURE;

  if (optind < argc)
    opts.dst_addr = argv[optind];

  hints->ep_attr->type = FI_EP_RDM;
  hints->caps = FI_MSG | FI_RMA | FI_RMA_EVENT;
  hints->mode = FI_CONTEXT;
  hints->domain_attr->mr_mode = FI_MR_LOCAL | OFI_MR_BASIC_MAP;

  ret = run_test();

  ft_free_res();
  return ft_exit_code(ret);
}
