#include <inttypes.h>
#include <stdbool.h>
#include "hmem.h"

static bool hmem_initialized = false;

struct ft_hmem_ops {
	int (*init)(void);
	int (*cleanup)(void);
	int (*alloc)(uint64_t device, void **buf, size_t size);
	int (*free)(void *buf);
	int (*memset)(uint64_t device, void *buf, int value, size_t size);
	int (*copy_to_hmem)(uint64_t device, void *dst, const void *src,
			    size_t size);
	int (*copy_from_hmem)(uint64_t device, void *dst, const void *src,
			      size_t size);
};

static struct ft_hmem_ops hmem_ops[] = {
	[FI_HMEM_SYSTEM] = {
		.init = ft_host_init,
		.cleanup = ft_host_cleanup,
		.alloc = ft_host_alloc,
		.free = ft_host_free,
		.memset = ft_host_memset,
		.copy_to_hmem = ft_host_memcpy,
		.copy_from_hmem = ft_host_memcpy,
  }};

int ft_hmem_init(enum fi_hmem_iface iface)
{
	int ret;

	ret = hmem_ops[iface].init();
	if (ret == FI_SUCCESS)
		hmem_initialized = true;

	return ret;
}

int ft_hmem_cleanup(enum fi_hmem_iface iface)
{
	int ret = FI_SUCCESS;

	if (hmem_initialized) {
		ret = hmem_ops[iface].cleanup();
		if (ret == FI_SUCCESS)
			hmem_initialized = false;
	}

	return ret;
}

int ft_hmem_alloc(enum fi_hmem_iface iface, uint64_t device, void **buf,
		  size_t size)
{
	return hmem_ops[iface].alloc(device, buf, size);
}

int ft_hmem_free(enum fi_hmem_iface iface, void *buf)
{
	return hmem_ops[iface].free(buf);
}

int ft_hmem_memset(enum fi_hmem_iface iface, uint64_t device, void *buf,
		   int value, size_t size)
{
	return hmem_ops[iface].memset(device, buf, value, size);
}

int ft_hmem_copy_to(enum fi_hmem_iface iface, uint64_t device, void *dst,
		    const void *src, size_t size)
{
	return hmem_ops[iface].copy_to_hmem(device, dst, src, size);
}

int ft_hmem_copy_from(enum fi_hmem_iface iface, uint64_t device, void *dst,
		      const void *src, size_t size)
{
	return hmem_ops[iface].copy_from_hmem(device, dst, src, size);
}
