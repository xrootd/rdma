/*
 * Copyright (c) 2020 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under the BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 */

#ifndef _HMEM_H_
#define _HMEM_H_

#include <stdlib.h>
#include <string.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>

enum fi_hmem_iface {
  FI_HMEM_SYSTEM = 0,
  FI_HMEM_CUDA,
  FI_HMEM_ROCR,
  FI_HMEM_ZE,
};

static inline int ft_host_init(void)
{
  return FI_SUCCESS;
}

static inline int ft_host_cleanup(void)
{
  return FI_SUCCESS;
}

static inline int ft_host_alloc(uint64_t device, void** buffer, size_t size)
{
  *buffer = malloc(size);
  return !*buffer ? -FI_ENOMEM : FI_SUCCESS;
}

static inline int ft_host_free(void* buf)
{
  free(buf);
  return FI_SUCCESS;
}

static inline int ft_host_memset(uint64_t device, void* buf, int value,
                                 size_t size)
{
  memset(buf, value, size);
  return FI_SUCCESS;
}

static inline int ft_host_memcpy(uint64_t device, void* dst, const void* src,
                                 size_t size)
{
  memcpy(dst, src, size);
  return FI_SUCCESS;
}

int ft_hmem_init(enum fi_hmem_iface iface);
int ft_hmem_cleanup(enum fi_hmem_iface iface);
int ft_hmem_alloc(enum fi_hmem_iface iface, uint64_t device, void** buf,
                  size_t size);
int ft_hmem_free(enum fi_hmem_iface iface, void* buf);
int ft_hmem_memset(enum fi_hmem_iface iface, uint64_t device, void* buf,
                   int value, size_t size);
int ft_hmem_copy_to(enum fi_hmem_iface iface, uint64_t device, void* dst,
                    const void* src, size_t size);
int ft_hmem_copy_from(enum fi_hmem_iface iface, uint64_t device, void* dst,
                      const void* src, size_t size);

#endif /* _HMEM_H_ */
