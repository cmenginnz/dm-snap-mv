obj-m := dm-snap-mv.o

PWD:=$(shell pwd)
KERNELDIR=/lib/modules/$(shell uname -r)/build

#EXTRA_CFLAGS := -O0 -DCONFIG_DM_DEBUG -fno-inline

all:
	$(MAKE) -C $(KERNELDIR) M=$(PWD)

clean:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) clean
