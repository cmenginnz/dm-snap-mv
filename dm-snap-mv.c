/*
 * Copyright (C) 2010 NOVELL, Inc.
 *
 * Module Author: Cong Meng, NOVELL
 *
 * This file is released under the GPL.
 */



#include "dm-snap-mv.h"

#include <linux/version.h>
#include <linux/slab.h>
#include <linux/device-mapper.h>
#include <linux/buffer_head.h>
#include <linux/bit_spinlock.h>
#include <linux/kthread.h>

/*
 *
 * the relationship of the major data structs
 *   +--> dmd --> orig --> cow --> job_queue
 *   |            *
 *   |            |
 *   |            +--> rollback_info --+
 *   |            |                    |
 *   |            +--> clean_info -----+--*> job
 *   |                                 |
 *   +---------------- map_info -------+
 *  
 *   note:
 *     ---> : pointer    
 *     --*> : embedded
 *
 */


/*
 *  4: size of [n, 0]
 * 12: size of one more co-vc
 *  6: size of co or vc
 *
 * if x/10 use up, count as full, which improves the performance by reduce moving elist
 */
#define DMS_ELIST_FULL_THRESHOLD (((4096 - 4 - 12) / 6) * 7/10)


#define BH_DMS_LOCK (BH_PrivateStart)
#define BH_DMS_WRITTING (BH_PrivateStart + 1)
#define BH_DMS_READING (BH_PrivateStart + 2)


#define DMS_LOCK_BH(bh, flags) \
	local_irq_save(flags);\
	bit_spin_lock(BH_DMS_LOCK, &(bh->b_state))


#define DMS_UNLOCK_BH(bh, flags) \
	bit_spin_unlock(BH_DMS_LOCK, &(bh->b_state));\
	local_irq_restore(flags)


// ex: MIDBITS32(AABBBCCCC, 3, 4) = BBB
#define MIDBITS32(x, len, right_len) (((uint32)(x) >> (right_len)) & ((1U << (len)) - 1))
#define RIGHTBITS32(x, len) ((uint32)(x) & ((1U << (len)) - 1))
#define RIGHTBITS64(x, len) ((uint64)(x) & ((1LLU << ((uint64)len)) - 1LLU))


// calc the Bitmap Page NR of the PLOT_NR
#define PLOT_2_BMP_PAGE_IDX(plot_nr) ((plot_nr) >> 14)
#define PLOT_2_BMP_IDX(plot_nr) (RIGHTBITS32(plot_nr, 14))

#define CHUNK_2_BMP_PAGE_IDX(chunk_nr) ((chunk_nr) >> 15)
#define CHUNK_2_BMP_IDX(chunk_nr) (RIGHTBITS32(chunk_nr, 15))


/*
 * one index2 plot contains 1K elist-pointers
 * one index1 plot contains 1K index2-pointers
 * so, one index1 plot contains 1M(2^20)  elist-pointers
 */
// convert chunk_dmd to index1 page index
#define CHUNK_2_INDEX1_PAGE_IDX(chunk_dmd) (chunk_dmd >> 20)
// convert chunk_dmd to subscript of index1 page
#define CHUNK_2_INDEX1(chunk_dmd) (MIDBITS32(chunk_dmd,  10, 10))
// convert chunk_dmd to subscript of index2 plot
#define CHUNK_2_INDEX2(chunk_dmd) (RIGHTBITS32(chunk_dmd, 10))


#define ERETRY (-(MAX_ERRNO + 1))

#define IS_RETRY(bh) ((bh) ==( typeof(bh))(ERETRY))
#define IS_EXX(bh) ((bh) >= (typeof(bh))(-MAX_ERRNO))

#define RET_RETRY(bh) if (IS_RETRY((bh))) return ERETRY
#define RET_RETRY_bh(bh) if (IS_RETRY((bh))) return ((dms_bh_t*)ERETRY)
#define CONT_RETRY(bh) if (IS_RETRY((bh))) continue
#define REMB_RETRY(ret, bh) if (IS_RETRY(bh)) (ret) = (bh);
#define RET_EXX(bh) if (IS_EXX((bh))) return (bh)
#define RET_RETRY_EXX(bh) RET_EXX((bh)); RET_RETRY((bh))


/*
 * convert version from 1-Dimension to 2-Dimension
 * local variable 'orig' must be defined
 * sizeof(dms_vtree_disk_t) << 7 = 4096
 */
#define VTREE_DISK(ver_nr) orig->vtree.disk[(ver_nr) >> 7][RIGHTBITS32((ver_nr), 7)]
#define VTREE_BH(ver_nr) orig->vtree.bh[(ver_nr) >> 7]

// local variable 'orig' must be defined
#define INDEX1_DISK(chunk_dmd) \
	orig->index1.disk[CHUNK_2_INDEX1_PAGE_IDX(chunk_dmd)][CHUNK_2_INDEX1(chunk_dmd)]

#define INDEX1_BH(chunk_dmd) \
	orig->index1.bh[CHUNK_2_INDEX1_PAGE_IDX(chunk_dmd)]

#define INDEX1_DISK_BY_IDX(index1_idx) \
	orig->index1.disk[index1_idx >> 10][RIGHTBITS32(index1_idx, 10)]

#define INDEX1_BH_BY_IDX(index1_idx) \
	orig->index1.bh[index1_idx >> 10]


#define LOCK_ORIG() down(&orig->orig_lock)
#define UNLOCK_ORIG() up(&orig->orig_lock)


#define BIO_GROUP_HASH_LEN 1024
#define BIO_GROUP_HASH_FUN(x) ((x) & (BIO_GROUP_HASH_LEN - 1))


/*
 *    | b2 b1
 * ---+-------
 *  0 |  R  O
 *  1 |  W  S
 */
#define DMS_MAP_TYPE_RO 0
#define DMS_MAP_TYPE_RS 1
#define DMS_MAP_TYPE_WO 2
#define DMS_MAP_TYPE_WS 3


#define DMS_JOB_TYPE_MAP 0
#define DMS_JOB_TYPE_CLEAN 1
#define DMS_JOB_TYPE_ROLLBACK 2


typedef struct buffer_head dms_bh_t;
typedef struct block_device dms_bdev_t;
typedef struct bio dms_bio_t;


struct list_head dms_origs_lh = LIST_HEAD_INIT(dms_origs_lh);
struct list_head dms_cows_lh = LIST_HEAD_INIT(dms_cows_lh);
struct list_head dms_dmds_lh = LIST_HEAD_INIT(dms_dmds_lh);

// protect the 3 lists above
struct semaphore dms_global_list_lock = __SEMAPHORE_INITIALIZER(dms_global_list_lock, 1);


#define LOCK_G_LISTS() down(&dms_global_list_lock)
#define UNLOCK_G_LISTS() up(&dms_global_list_lock)


typedef struct dms_list_s {
	struct dms_list_s *next;
	void *load;
} dms_list_t;


typedef struct {
	struct list_head node;
	dms_bh_t *bh;
	dms_list_t *job_head;
} dms_defer_bh_t;


typedef struct 
{
	dms_bh_t *bh;
	dms_misc_disk_t* disk;
} dms_misc_t;


typedef struct
{
	dms_vtree_aux_t auxiliary[MAX_NR_OF_VERSIONS];
	dms_bh_t *bh[VTREE_SLOT_SIZE];
	dms_vtree_disk_t *disk[VTREE_SLOT_SIZE];
} dms_vtree_t;


typedef struct
{
	dms_bh_t *bh[INDEX1_SLOT_SIZE];
	plot_t *disk[INDEX1_SLOT_SIZE];
} dms_index1_t;


typedef struct
{
	dms_bh_t *bh[PLOT_BITMAP_ZONE_SIZE];
	void *disk[PLOT_BITMAP_ZONE_SIZE];
} dms_plot_bitmap_t;


typedef struct
{
	dms_bh_t *bh[CHUNK_BITMAP_ZONE_SIZE];
	void *disk[CHUNK_BITMAP_ZONE_SIZE];
} dms_chunk_bitmap_t;


// Chunk-Copy status:
#define DMS_CC_TO_READ 0
#define DMS_CC_TO_WRITE 1
#define DMS_CC_DONE 2

typedef struct
{
	int status;
	dms_bio_t *bio;
	dms_bdev_t *bdev_s, *bdev_d;
	chunk_t chunk_s, chunk_d;
} dms_chunk_copy_t;


typedef struct
{
	int job_type;

	struct list_head job_node;

	// the count of bh IOs this job is waiting
	atomic_t io_count;

	// there are some places will cause job error in job context:
	// BH async end, BIO end, xxx_expand, relocate_tag
	int error;

	// buffer head list, to be put_bh() while bio done
	dms_list_t *read_bh_list;
} dms_job_t;


/*
 * cow ID
 * cid is embedded in the thread name: "dms_x_sda1". so it can be get by 'current'
 */
#define g_sid (current->comm[4] - '0')

dms_job_t* g_jobs[DMS_MAX_NR_COWS];

#define g_job g_jobs[g_sid]


typedef struct
{
	atomic_t gogo;

	// 1: signal to quit
	// 2: the thread has quited
	atomic_t quit;
	
	// list head of job queue
	//(list node: job->job_node)
	struct list_head list;
	spinlock_t list_lock;

	struct task_struct *process_task;
} dms_job_thread_t;


typedef struct
{
	dms_job_t job;

	// the index of index1 pointing to the index2 plot to clean
	uint32 index1_idx;

	// the index2 plot nr to clean
	// if non-zero, current index2 plot is undler cleaning
	plot_t index2_plot_nr;

	// the bh of the index2 plot
	dms_bh_t *index2_plot_bh;

	// point to the beginning of index2 plot
	plot_t *p_index2_plot;
} dms_clean_info_t;


typedef struct 
{
	dms_job_t job;

	// the dmsetup user-space command waiting for rollback finish
	struct task_struct *process_task;

	// the tag of snapshot to rollback
	// non-zero: rollbacking is in progress
	uint16 tag;

	uint16 ver_nr;

	// the index of index1 pointing to the index2 plot to rollback
	uint32 index1_idx;

	// the index2 plot nr to rollback
	// if non-zero, current index2 plot is undler rollbacking
	plot_t index2_plot_nr;

	// the bh of the index2 plot
	dms_bh_t *index2_plot_bh;

	// the pointer of the page of the bh aboves
	plot_t *p_index2_plot;

	// for chunk copy
	chunk_t chunk_alloc[1024];
	dms_chunk_copy_t chunk_copy1[1024], chunk_copy2[1024];

	uint32 plot_rollback_done[1024/32];	
} dms_rollback_info_t;



typedef struct
{
	// cow id. start from 0. one cow has one job thread.
	int cow_id;
	
	// cow reference count
	atomic_t count;

	// cow device
	dms_bdev_t *bdev;

	// node in dms_cows_lh
	struct list_head cows_ln;

	// job thread info
	dms_job_thread_t job_thread;

	// make (job->io_count dec + wake up job) and (job->io_count read) be mutual exclusive
	spinlock_t wake_iocount_lock;

	// cow device metadata
	dms_misc_t misc;
	dms_plot_bitmap_t plot_bitmap;
	dms_chunk_bitmap_t chunk_bitmap;

	chunk_t next_alloc_chunk;

	// 2 ^ sector_shift sectors per chunk
	//chunk_nr = sector_nr >> sector_shift
	uint32 sector_shift;

	struct list_head defer_bh_head;
} dms_cow_t;


typedef struct
{
	int freeing;

	uint32 slot_id;
	dms_bdev_t *bdev;

	// orig device size in sector	
	uint64 orig_size_in_sect;  

	// max chunk number of orig dev
	uint32 max_chunk_nr;  

	// max index of index1 in index1 slot
	uint32 max_index1_idx;  

	// node in dms_origs_lh
	struct list_head origs_ln; 

	dms_vtree_t vtree;
	dms_index1_t index1;

	dms_cow_t *cow;

	atomic_t count;

	dms_clean_info_t clean_info;
	dms_rollback_info_t rollback_info;

	// make job and dmsetup-msg-commands be mutual exclusive
	struct semaphore orig_lock;
} dms_orig_t;


/*
 * one device-mapper device corresponds to one dms_dm_dev_t
 */
typedef struct
{
	dms_orig_t *orig;
	int tag;
	// ver should be inited in dms_dmd_alloc(), 
	// might be updated in write_snap() and vtree_clean()
	atomic_t ver;
	char const *name;
	// created_verstions_list_node of dms_dmds_lh
	struct list_head dmds_ln;  
	struct dm_dev *dd_orig, *dd_cow;

	// WO does NOT need bio group
	struct list_head bio_group_hash [3][BIO_GROUP_HASH_LEN];
} dms_dm_dev_t;


typedef struct
{
	dms_job_t job;

	dms_dm_dev_t *dmd;

	int map_type;

	dms_list_t *dmd_bios;
	uint32 bios_size;  // in sector

	struct list_head group_bio_node;
	
	// used for chunk copy
	dms_chunk_copy_t chunk_copy;

	int done;

	chunk_t chunk_dmd;

	// WO always sets 0 here
	// WS & RS set the refered chunk number here for map_and_submit_bios()
	chunk_t  chunk_cow;

	/*
	 * dms_map_cow() temporarily save newly allocated chunk number here, and clean to 0 after set_exception() done
	 * map_job_done() should free the chunk if it's not zero
	 */
	chunk_t  chunk_alloc;

	int pre_set_excep;

	atomic_t ws_dmd_bio_count;
	spinlock_t ws_dms_bio_lock;
} dms_map_info_t;


typedef struct
{
	// 'dmsetup create' commands
	int create_orig, create_snap;

	// 'dmsetup message' commands
	int snap_orig, snap_snap, del_orig, del_snap, rollback_snap;

	// positional arguments
	int tag;
	char *orig_dev_name, *cow_dev_name;

	// options
	char *memo, *orig_dev_id;
	int format_cow, force_format_cow, bind, chunk_size, background;

	// for the dms_construct()
	struct dm_dev *dd_orig, *dd_cow;
} dms_parsed_args_t;


/*
 * three are 2 kinds of job_lists
 * 1. preemptive list
 * 2. bh->private: bh read/write waiting list
 */
typedef struct dms_job_list_s {
	dms_job_t *job;
	struct dms_job_list_s *next;
} dms_job_list_t;


void dms_cow_mark_invalid(dms_cow_t *cow);
void dms_dbg_dump_elist_bh(dms_bh_t *elist_bh);
int dms_elist_find_co(dms_bh_t *elist_bh, chunk_t chunk_dmd);


struct kmem_cache *dms_list_kmem_cache;


dms_list_t* dms_list_alloc(void *load)
{
	dms_list_t *dms_list;
	dms_list = kmem_cache_alloc(dms_list_kmem_cache, GFP_ATOMIC);
	dms_list->load = load;
	
	return dms_list;
}


void dms_list_free(dms_list_t *dms_list)
{
	kmem_cache_free(dms_list_kmem_cache, dms_list);
}


/*
 * head -> ...  =>   head -> node -> ...
 */
void dms_list_add(void **head, dms_list_t *node)
{
	node->next = *head;
	*head = node;
}


/*
 * dequeue and free the 1st node of list @head
 *
 * return a load pointer if list is not empty
 * return 0 if list is empty
 */
void* dms_list_fetch_load(dms_list_t **head)
{
	dms_list_t *temp;
	void *load = 0;

	if (*head) {
		temp = *head;
		*head = temp->next;
		load = temp->load;
		dms_list_free(temp);
	}
	return load;
}

	
void dms_list_add_load(dms_list_t **head, void* load)
{
	dms_list_t *node;
	node = dms_list_alloc(load);
	node->next = *head;
	*head = node;
}


time_t dms_get_time(void)
{
	struct timeval tv;
	do_gettimeofday(&tv);
	return tv.tv_sec;
}


void* dms_kmalloc(int size, char *name)
{
	void *p;
	uint32 count = 0;

	while (1) {
		count++;
		p = kmalloc(size, GFP_ATOMIC);
		if (p) break;
		if ((count & 1023) == 0)
			DMERR("tried to kmalloc %u-B %s %u times", size, name ? name : "", count);
	}

	return p;
}


struct page* dms_page_loop_alloc(void)
{
	int count = 0;
	struct page *p;

	while (1) {
		count++;
		p = alloc_page(GFP_NOIO);
		if (p) break;
		if ((count & 1023) == 0)
			DMERR("tried alloc a page %u times", count);
	}

	return p;
}


/*
 * @x = 1 << r
 *
 * return r
 */
int dms_shift(uint32 x)
{
	// fls(4)=3  (Find Last Set bit)  4 = 100
	return fls(x) - 1;
}


/*
 * chunk_nr = sector_nr >> sector_shift
 * (1 << sector_shift) sects per chunk
 *
 * chunk_size: in page
 * ex: sector_shift(2) -> 4,  1->3, 16->7
 */
int sector_shift(int chunk_size)
{
	return dms_shift(chunk_size) + dms_shift(8);
}


void dms_bh_page_copy(dms_bh_t *bh_dst, dms_bh_t *bh_src)
{
	memcpy(page_address(bh_dst->b_page), page_address(bh_src->b_page), PAGE_SIZE);
}


void dms_zero_page(struct page *page)
{
	memset(page_address(page), 0, PAGE_SIZE);
}


int dms_get_device(struct dm_target *ti, char *dev_name, struct dm_dev **dd)
{
	int r;

	#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 34)
	r = dm_get_device(ti, dev_name, 0, ti->len, dm_table_get_mode(ti->table), dd);
	#else
	r = dm_get_device(ti, dev_name, dm_table_get_mode(ti->table), dd);
	#endif

	if (r)
		DMERR("get %s error", dev_name);
	
	return r;
}


/*
 * get the size of @bdev in sectors
 */
sector_t dms_bdev_size_sect(dms_bdev_t *bdev)
{
	return bdev->bd_part ? bdev->bd_part->nr_sects : bdev->bd_disk->part0.nr_sects;	
}


/*
 * similar to snprintf, except that it prints at the end of @buf
 * @size: the maximal length(including '\0') of @buf
 */
void dms_snprintf(char *buf, int size, const char *fmt, ...)
{
	int len;
	va_list args;

	len = strlen(buf);
	size = size - len;

	if (size > 0) {
		va_start(args, fmt);
		vsnprintf((buf + len), size, fmt, args);	
		va_end(args);
	}
}


/*
 * return a string pointing the name of @bdev, which is saved in static memory
 */
char* dms_bdev_name(dms_bdev_t *bdev)
{
	static int idx = 0;
	static char name[4][64] = {{0}, {0}, {0}, {0}};

	idx = (idx + 1) & 3;
	name[idx][0] = 0;
	
	dms_snprintf(name[idx], 64, bdev->bd_disk->disk_name);

	if (bdev->bd_part && bdev->bd_part->partno)
		dms_snprintf(name[idx], 64, "%d", bdev->bd_part->partno);
	
	return name[idx];
}


/*
 * return a string pointing the major:minor of @bdev, which is saved in static memory
 */
char* dms_bdev_mm(dms_bdev_t *bdev)
{
	static int idx = 0;
	static char name[4][16] = {{0}, {0}, {0}, {0}};

	idx = (idx + 1) & 3;
	name[idx][0] = 0;

	dms_snprintf(name[idx], 16, "%d:%d", MAJOR(bdev->bd_dev), MINOR(bdev->bd_dev));

	return name[idx];
}


char const* dms_dm_name(struct dm_target *ti)
{
	char const* name;
	struct mapped_device *md;

	md = dm_table_get_md(ti->table);
	name = dm_device_name(md);
	
	#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 34)
	dm_put(md);
	#endif

	return name;
}


void dms_io_err(dms_bdev_t *bdev, char *str)
{
	DMERR("%s IO error on %s", str, dms_bdev_name(bdev));
}


/*
 * convert page idx to block number
 */
sector_t dms_page_2_block(dms_bdev_t *bdev, uint32 page_idx)
{
	uint64 shift;
	sector_t block;

	shift = PAGE_CACHE_SHIFT - bdev->bd_inode->i_blkbits;
	block = (sector_t)(page_idx) <<  shift;
	
	return block;
}


inline void dms_write_bit(void *addr, uint32 nr, uint32 bit)
{
	if (bit)
		__set_bit(nr, addr);
	else
		__clear_bit(nr, addr);
}


inline uint32 dms_read_bit(void *addr, uint32 nr)
{
	return test_bit(nr, addr) ? 1 : 0;
}


/*
 *  @addr:
 *  low <-                -> high
 *
 *   0 1   2 3   4    5      6 7  (bit)
 *  [. .] [. .] [bit0 bit1] [. .]
 *    0     1        2        3
 *                   |
 *                   @nr
 *
 *  @bits = (bit0 << 1) | bit1
 */
inline void dms_write_2bits(void *addr, uint32 nr, uint32 bits)
{
	uint32 bit0, bit1;

	bit0 = bits >> 1;
	bit1 = bits & 0x1;

	dms_write_bit(addr, nr << 1, bit0);
	dms_write_bit(addr, (nr << 1) | 1, bit1);
}


inline uint32 dms_read_2bits(void *addr, uint32 nr)
{
	uint32 bits, bit0, bit1;

	bit0 = dms_read_bit(addr, nr << 1);
	bit1 = dms_read_bit(addr, (nr << 1) | 1);
	bits = (bit0 << 1) | bit1;
	return bits;
}


/*
 * CBZS: Chunk_Bitmap_Zone_Size                              256
 * I1SS: Index1_Slot_Size                                    8
 *  MCS: Max_Chunk_Size                                      16
 *
 *  MOPN: Max Orig Page Number
 *  MCPN: Max Cow Page Number
 * CICS: Command Inputed Chunk Size
 *
 * MOCN: Max Orig Chunk Number
 * MCCN: Max Cow Chunk Number
 * 
 *   CS: Chunk Size
 *
 * all above are in the unit of page
 *
 * 1 index1 page can index 2^20 orig chunks
 * 1 chunk bitmap page has 2^15 bits
 *
 * restriction:
 *   MOCN = min{ uint32(-1), (I1SS << 20) - 1) }             2^23-1
 *   MCCN = min{ uint32(ERETRY - 1), (CBZS << 15) - 1 }      2^22-1
 *
 *   (MOPN >> s) <= MOCN
 *   (MCPN >> s) <= MCCN
 *
 * result:
 *   (1 << min(s)) <= CS <= MCS
 *
 * calc the chunk size in page
 *
 * return a positive number if ok
 * return 0 if error
 */
int dms_calc_chunk_size(dms_parsed_args_t *pargs)
{
	uint32 cbzs = CHUNK_BITMAP_ZONE_SIZE;
	uint32 i1ss = INDEX1_SLOT_SIZE;
	uint32 mcs = MAX_CHUNK_SIZE;
	
	uint32 cics = pargs->chunk_size;
	uint64 mopn = dms_bdev_size_sect(pargs->dd_orig->bdev) >> 3LLU;
	uint64 mcpn = dms_bdev_size_sect(pargs->dd_cow->bdev) >> 3LLU;

	uint32 mocn = min(
		(uint32)(ERETRY - 1),
		(cbzs << 15) - 1
	);

	uint32 mccn = min(
		(uint32)(-1),
		(i1ss << 20) - 1
	);

	uint64 s = 0;
	uint32 cs;

	do {
		if (((mcpn >> s) <= mccn) && ((mopn >> s) <= mocn))
			break;
		s++;
	} while (1);

	cs = 1 << s;

	if (cs > mcs) {
		DMERR("calc chunk size error. cs=%u mopn=%llu mcpn=%llu", cs, mopn, mcpn);
		return 0;
	}

	if (cics) {
		if ((cs <= cics) && (cics <= mcs))
			return cics;

		DMERR("invalid option -c %d. %u <= c <= %u", cics, cs, mcs);
		return 0;
	}

	return cs;
}


int dms_dbg_scan_job_queue(dms_cow_t *cow)
{
	int i;
	unsigned long flags;
	struct list_head *next, *curr;
	dms_job_thread_t *jt = &cow->job_thread;
	dms_job_t *job;
	uint32 count[3];
	uint32 total;

	spin_lock_irqsave(&jt->list_lock, flags);

	DMINFO("job thread: gogo=%u quit=%d", atomic_read(&jt->gogo), atomic_read(&jt->quit));

	total = 0;
	memset(count, 0, sizeof(count));
	next = jt->list.next;

	while (next != &jt->list) {
		// 'curr' may be freed by run_job(), so save 'next' in advance
		curr = next;
		next = next->next;

		job = list_entry(curr, dms_job_t, job_node);

		total++;
		count[job->job_type]++;

		if (total <= 3 || job->job_type)
			DMINFO("job: type=%d io-count=%u err=%d",
				job->job_type, atomic_read(&job->io_count), job->error
			);
	}

	for (i = 0; i < 3; i++)
		DMINFO("job type:%d count:%u", i, count[i]);

	spin_unlock_irqrestore(&jt->list_lock, flags);

	return 0;
}


char* dms_str_args(unsigned int argc, char **argv)
{
	int i;
	static char buf[256];

	buf[0] = 0;
	
	for (i = 0; i < argc; i++)
		dms_snprintf(buf, 256, "%s ", argv[i]);

	return buf;
}


/*
 * merge the memo who might Scatter in the last few argv
 * @p: argv[p] is the 1st segment of the memo
 *
 * return a char pointer which point to the merged memo string
 */
char* dms_merge_memo(unsigned int argc, char **argv, int p)
{
	int i;

	for (i = p + 1; i < argc; i++) {
		argv[i][-1] = ' ';
	}

	return argv[p];
}


/*
 * return 1 if @str1 is equal to @str2
 */
int dms_strequ(char* str1, char* str2)
{
	return !strcmp(str1, str2);
}


/*
 * return 0 if ok
 * return -EINVAL if error
 */
static int dms_parse_message_args(unsigned int argc, char **argv, dms_parsed_args_t *pargs)
{
	int i;

	if (!argc) goto bad;

	memset(pargs, 0, sizeof(dms_parsed_args_t));

	/*
	 * Snap Orig
	 * so [-m <memo>]
	 */
	if (dms_strequ(argv[0], "so")) {
		if (argc < 1) goto bad;

		pargs->snap_orig = 1;

		for (i = 1; i < argc; i++) {
			if (dms_strequ(argv[i], "-m")) {
				pargs->memo = dms_merge_memo(argc, argv, i + 1);
				break;
			} else
				goto bad;
		}

		goto ok;
	}

	/*
	 * Snap Snap
	 * ss <tag> [-m <memo>]
	 */
	if (dms_strequ(argv[0], "ss")) {
		if (argc < 2) goto bad;

		pargs->snap_snap = 1;
		if (!sscanf(argv[1], "%u", &pargs->tag)) goto bad;

		for (i = 2; i < argc; i++) {
			if (dms_strequ(argv[i], "-m")) {
				pargs->memo = dms_merge_memo(argc, argv, i + 1);
				break;
			} else
				goto bad;
		}

		goto ok;
	}

	/*
	 * Delete Orig
	 * do
	 */
	if (dms_strequ(argv[0], "do")) {
		if (argc != 1) goto bad;

		pargs->del_orig = 1;

		goto ok;
	}

	/*
	 * Delete Snapshot
	 * ds <tag>
	 */
	if (dms_strequ(argv[0], "ds")) {
		if (argc != 2) goto bad;

		pargs->del_snap = 1;
		if (!sscanf(argv[1], "%u", &pargs->tag)) goto bad;

		goto ok;
	}

	/*
	 * Rollback Snapshot
	 * rs <tag> [-b]
	 */
	if (dms_strequ(argv[0], "rs")) {
		if (argc < 2) goto bad;

		pargs->rollback_snap = 1;
		if (!sscanf(argv[1], "%u", &pargs->tag)) goto bad;

		for (i = 2; i < argc; i++) {
			if (dms_strequ(argv[i], "-b"))
				pargs->background = 1;
			else
				goto bad;
		}

		goto ok;
	}

bad:
	DMERR("invalid dms message arguments: %s", dms_str_args(argc, argv));
	return -EINVAL;
ok:
	return 0;
}


/*
 * return 0 if ok
 * return -EINVAL if error
 */
static int dms_parse_construct_args(unsigned int argc, char **argv, dms_parsed_args_t *pargs)
{
	int i;

	if (!argc) goto bad;

	memset(pargs, 0, sizeof(dms_parsed_args_t));

	/*
	 * Create Orig dm_dev
 	 * co <orig_dev> <cow_dev> [-i <orig_id>] [-b [-F|-f [-c <chunk_size>]]]
	 * <orig_dev> is the used as orig_id if <orig_id> not specified
	 */
	if (dms_strequ(argv[0], "co")) {
		if (argc < 3) goto bad;

		pargs->create_orig = 1;
		pargs->tag = 0;
		pargs->orig_dev_name = argv[1];
		pargs->cow_dev_name = argv[2];

		pargs->orig_dev_id = argv[1];

		for (i = 3; i < argc; i++) {
			if (dms_strequ(argv[i], "-i"))
				pargs->orig_dev_id = argv[++i];
			else if (dms_strequ(argv[i], "-b")) 
				pargs->bind = 1;
			else if (dms_strequ(argv[i], "-F")) 
				pargs->force_format_cow = 1;
			else if (dms_strequ(argv[i], "-f"))
				pargs->format_cow = 1;
			else if (dms_strequ(argv[i], "-c")) {
				if (!sscanf(argv[++i], "%u", &pargs->chunk_size)) goto bad;
			} else
				goto bad;
		}
		goto ok;
	}

	/*
	 * Create Snap dm_dev
	 * cs <orig_dev> <cow_dev> <tag> [-i <orig_id>]
	 * <orig_dev> is the used as orig_id if <orig_id> not specified
	 */
	if (dms_strequ(argv[0], "cs")) {
		if (argc < 4) goto bad;

		pargs->create_snap = 1;
		pargs->orig_dev_name = argv[1];
		pargs->cow_dev_name = argv[2];
       		if (!sscanf(argv[3], "%u", &pargs->tag)) goto bad;

		pargs->orig_dev_id = argv[1];

		for (i = 4; i < argc; i++) {
			if (dms_strequ(argv[i], "-i"))
				pargs->orig_dev_id = argv[++i];
			else
				goto bad;
		}
		
		goto ok;
	}

bad:
	DMERR("invalid dms create arguments: %s", dms_str_args(argc, argv));
	return -EINVAL;
ok:
	return 0;
}


/****************
 *
 * job 1/2
 *
 ****************/


#define JOB_2_MI(job) (container_of((job), dms_map_info_t, job))
#define JOB_2_CI(job) (container_of((job), dms_clean_info_t, job))
#define JOB_2_RI(job) (container_of((job), dms_rollback_info_t, job))


/*
 * get orig by job
 */
dms_orig_t* dms_job_2_orig(dms_job_t *job)
{
	dms_orig_t *orig = 0;

	dms_map_info_t *mi;
	dms_clean_info_t *ci;
	dms_rollback_info_t *ri;

	switch (job->job_type)
	{
	case DMS_JOB_TYPE_MAP:
		mi = JOB_2_MI(job);
		orig = mi->dmd->orig;
		break;
	case DMS_JOB_TYPE_CLEAN:
		ci = JOB_2_CI(job);
		orig = container_of(ci, dms_orig_t, clean_info);
		break;
	case DMS_JOB_TYPE_ROLLBACK:
		ri = JOB_2_RI(job);
		orig = container_of(ri, dms_orig_t, rollback_info);
		break;
	}

	return orig;
}


int dms_job_io_done(dms_job_t *job)
{
	int r;
	dms_orig_t *orig;
	dms_cow_t *cow;
	unsigned long flags;

	orig = dms_job_2_orig(job);
	cow = orig->cow;

	spin_lock_irqsave(&cow->wake_iocount_lock, flags);
	r = !atomic_read(&job->io_count);
	spin_unlock_irqrestore(&cow->wake_iocount_lock, flags);

	return r;
}


/*
 * wake up job thread by cow
 */
void dms_job_do_wake_up(dms_cow_t *cow)
{
	atomic_set(&cow->job_thread.gogo, 1);
	wake_up_process(cow->job_thread.process_task);
}


/*
 * wake up job thread by cow
 */
void dms_job_wake_up(dms_cow_t *cow)
{
	unsigned long flags;

	spin_lock_irqsave(&cow->wake_iocount_lock, flags);
	
	dms_job_do_wake_up(cow);

	spin_unlock_irqrestore(&cow->wake_iocount_lock, flags);
}


/*
 * dec the job IO count, and wake up job thread if job has no io count
 */
void dms_job_dec_and_wake(dms_job_t *job)
{
	dms_orig_t *orig;
	dms_cow_t *cow;
	unsigned long flags;

	orig = dms_job_2_orig(job);
	cow = orig->cow;

	spin_lock_irqsave(&cow->wake_iocount_lock, flags);
	atomic_dec(&job->io_count);
	dms_job_do_wake_up(cow);
	spin_unlock_irqrestore(&cow->wake_iocount_lock, flags);
}


void dms_job_plug(dms_cow_t *cow, dms_job_t *job)
{
	unsigned long flags;
	dms_job_thread_t *jt = &cow->job_thread;

	if (list_empty(&job->job_node)) {
		spin_lock_irqsave(&jt->list_lock, flags);
		list_add_tail(&job->job_node, &jt->list);
		spin_unlock_irqrestore(&jt->list_lock, flags);
	}
}


void dms_job_unplug(dms_cow_t *cow, dms_job_t *job)
{
	unsigned long flags;
	dms_job_thread_t *jt = &cow->job_thread;;

	spin_lock_irqsave(&jt->list_lock, flags);
	list_del_init(&job->job_node);
	spin_unlock_irqrestore(&jt->list_lock, flags);
}


void dms_job_done(dms_job_t *job)
{
	dms_bh_t *bh;
	dms_list_t **head = &job->read_bh_list;

	while ((bh = dms_list_fetch_load(head)))
		brelse(bh);
}


void dms_job_init(dms_job_t *job, int job_type)
{
	job->job_type = job_type;
	INIT_LIST_HEAD(&job->job_node);

	atomic_set(&job->io_count, 0);
}


/****************
 *
 * buffer head
 *
 ****************/


/*
 * get BH with ref count increased without reading.
 * 
 * return the bh with ref count incresed
 * never return error or NULL here
 */
dms_bh_t* dms_bh_get(dms_bdev_t *bdev, uint32 page_idx)
{
	sector_t block;
	dms_bh_t *bh;

	block = dms_page_2_block(bdev, page_idx);
	
	//__getblk returned buffer with ref count increased
	// it should not return NULL here, as PAGE_SIZE is mutiple of hard sect size
	bh = __getblk(bdev, block, PAGE_SIZE);

	return bh;
}


/*
 * wake up all jobs waiting for the bh
 */
void dms_bh_wake_jobs(dms_list_t *job_list, int uptodate)
{
	dms_job_t *job;
	dms_orig_t *orig;
	dms_cow_t *cow;
	unsigned long flags;

	job = job_list->load;

	orig = dms_job_2_orig(job);
	cow = orig->cow;

	spin_lock_irqsave(&cow->wake_iocount_lock, flags);

	while ((job = dms_list_fetch_load(&job_list))) {
		if (!uptodate) job->error = 1;
		atomic_dec(&job->io_count);
	}

	dms_job_do_wake_up(cow);

	spin_unlock_irqrestore(&cow->wake_iocount_lock, flags);	
}


/*
 * Async Write BH callback
 *
 * base on end_buffer_write_sync()
 */
void dms_bh_awrite_end(dms_bh_t *bh, int uptodate)
{
	dms_list_t *job_list;

	BUG_ON(!buffer_uptodate(bh));
	
	if (uptodate) {
		set_buffer_uptodate(bh);
	} else {
		set_buffer_write_io_error(bh);
		clear_buffer_uptodate(bh);
	}

	job_list = bh->b_private;
	bh->b_private = 0;

	unlock_buffer(bh);
	brelse(bh);

	dms_bh_wake_jobs(job_list, uptodate);
}


void dms_bh_awrite_locked(dms_bh_t *bh)
{
	get_bh(bh);
	bh->b_end_io = dms_bh_awrite_end;
	// FIXME: submit_bh() might return EXX
	submit_bh(WRITE, bh);
}


/*
 * cow->defer_bh_head <-> defer_bh <-> ...
 *                         +-bh
 *                         +-job_head-> <list> -next-> ...
 *                                        +-load-> job
 */

void dms_defer_bh(dms_cow_t *cow, dms_bh_t *bh)
{	
	dms_job_t *job = g_job;
	dms_defer_bh_t *defer_bh;

	// find exist defer_bh
	list_for_each_entry (defer_bh, &cow->defer_bh_head, node) {
		if (defer_bh->bh == bh)
			goto ok;
	}

	// not found. alloc one
	defer_bh = dms_kmalloc(sizeof(dms_defer_bh_t), "defer_bh");
	defer_bh->bh = bh;
	defer_bh->job_head = 0;
	list_add(&defer_bh->node, &cow->defer_bh_head);

ok:
	dms_list_add_load(&defer_bh->job_head, job);
	atomic_inc(&job->io_count);
}


void dms_flush_defer_bh(dms_cow_t *cow)
{
 	dms_defer_bh_t *defer_bh, *safe;

	list_for_each_entry_safe(defer_bh, safe, &cow->defer_bh_head, node) {
 		if (trylock_buffer(defer_bh->bh)) {
			defer_bh->bh->b_private = defer_bh->job_head;
			dms_bh_awrite_locked(defer_bh->bh);
			list_del(&defer_bh->node);
			kfree(defer_bh);			
		}
	}
}


void dms_dbg_thread_info(dms_cow_t *cow)
{
	int nr_job = 0;
	int nr_defer_bh = 0;
	int nr_ws_io = 0;
	int nr_io = 0;
 	dms_defer_bh_t *defer_bh;
	dms_job_t *job;
	dms_job_thread_t *jt;
	dms_map_info_t *mi;

	int err1 = 0, err2 = 0;

	jt = &cow->job_thread;

	list_for_each_entry(job, &jt->list, job_node) {
		nr_job++;

		nr_io += atomic_read(&job->io_count);
		if (atomic_read(&job->io_count) < 0) err1=1;

		mi = JOB_2_MI(job);
		nr_ws_io += atomic_read(&mi->ws_dmd_bio_count);
		if (atomic_read(&mi->ws_dmd_bio_count) < 0) err2=1;
	}

	list_for_each_entry(defer_bh, &cow->defer_bh_head, node) {
		nr_defer_bh++;
	}

	DMINFO("nr_derfer_bh=%d nr_job=%d nr_io=%d nr_ws_io=%d gogo=%d", 
		nr_defer_bh, nr_job, nr_io, nr_ws_io, atomic_read(&jt->gogo));

	BUG_ON(err1);
	BUG_ON(err2);
}


/*
 * Async Write BH
 *
 * base on sync_dirty_buffer()
 * this function might sleep in lock_buffer()
 */
void dms_bh_awrite(dms_cow_t *cow, dms_bh_t *bh)
{
	set_buffer_uptodate(bh);
	mark_buffer_dirty(bh);

	dms_defer_bh(cow, bh);
}


/*
 * get BH by Async Reading callback
 *
 * end_buffer_read_sync()
 */
void dms_bh_aread_end(dms_bh_t *bh, int uptodate)
{
	unsigned long flags;
	dms_list_t *job_list;

	if (uptodate)
		set_buffer_uptodate(bh);
	else
		clear_buffer_uptodate(bh);

	DMS_LOCK_BH(bh, flags);

	job_list = bh->b_private;
	bh->b_private = 0;
	clear_bit(BH_DMS_READING, &bh->b_state);

	DMS_UNLOCK_BH(bh, flags);	

	unlock_buffer(bh);
	brelse(bh);

	dms_bh_wake_jobs(job_list, uptodate);
}


/*
 * get BH by Async Reading
 *
 * be invoked in thread-context. 
 * non-concurrent function in cow-device-wide.
 * block function (dms_bh_get() might sleep)
 *
 * return the bh if it is uptodate (with the reference count of bh increased)
 * return ERETRY if reading
 */
dms_bh_t *dms_bh_aread(dms_bdev_t *bdev, uint32 page_idx)
{
	dms_job_t *job = g_job;
	unsigned long flags;
	dms_bh_t *bh;

	bh = dms_bh_get(bdev, page_idx);
	dms_list_add_load(&job->read_bh_list, bh);
	if (buffer_uptodate(bh)) goto ok;


	DMS_LOCK_BH(bh, flags);

	if (buffer_uptodate(bh)) {
		DMS_UNLOCK_BH(bh, flags);
		goto ok;
	}

	atomic_inc(&job->io_count);
	
	if (test_and_set_bit(BH_DMS_READING, &bh->b_state)) {
		dms_list_add_load((dms_list_t**)(&bh->b_private), job);
		DMS_UNLOCK_BH(bh, flags);
		goto retry;
	}

	bh->b_private = 0;
	dms_list_add_load((dms_list_t**)(&bh->b_private), job);

	DMS_UNLOCK_BH(bh, flags);


	lock_buffer(bh);
	get_bh(bh);
	bh->b_end_io = dms_bh_aread_end;

	// FIXME: submit_bh() might return EOPNOTSUPP
	submit_bh(READ, bh);

retry:
	return (dms_bh_t*)ERETRY;
ok:
	return bh;
}


/*
 * Sync Write BH
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_bh_swrite(dms_bh_t *bh)
{
	set_buffer_uptodate(bh);
	mark_buffer_dirty(bh);
	// sync_dirty_buffer() return 0 or EXX (EIO, EOPNOTSUPP, ...)
	return sync_dirty_buffer(bh);
}


/*
 * get BH by Sync Reading
 *
 * read a 4KB block
 *
 * return a bh if ok, with reference count increased
 * return NULL if error
 */
dms_bh_t *dms_bh_sread(dms_bdev_t *bdev, uint32 page_idx)
{
	dms_bh_t *bh;
	sector_t block;

	block = dms_page_2_block(bdev, page_idx);

	// __bread() return NULL if read error
	bh = __bread(bdev, block, PAGE_SIZE);

	return bh;
}


/*
 * get BH by No Reading
 *
 * should be called in job thread context
 * bh will be release in job_done()
 *
 * return a bh, which might be not uptodate, but ref count increased
 */
dms_bh_t* dms_bh_nread(dms_bdev_t *bdev, sector_t page_idx)
{
	dms_bh_t *bh;
	dms_job_t *job = g_job;

	bh = dms_bh_get(bdev, page_idx);

	set_buffer_uptodate(bh);

	dms_list_add_load(&job->read_bh_list, bh);

	return bh;
}


/*
 * get Plot BH by Async Reading
 *
 * return what dms_bh_aread() return
 *
 * return the bh if it is uptodate (with the reference count of bh increased)
 * return ERETRY if reading
 */
dms_bh_t* dms_bh_plot_aread(dms_orig_t *orig, plot_t plot_nr)
{
	dms_bh_t *bh;
	uint32 page_idx;

	page_idx = PLOT_ZONE_P + plot_nr;

	bh = dms_bh_aread(orig->cow->bdev, page_idx);
	RET_RETRY_bh(bh);

	return bh;
}


/*
 * get Plot BH by No Reading
 *
 * return what dms_bh_nread() return
 *
 * return a bh, which might be not uptodate, but ref count increased
 */
dms_bh_t* dms_bh_plot_nread(dms_orig_t *orig, plot_t plot_nr)
{
	uint32 page_idx;

	page_idx = PLOT_ZONE_P + plot_nr;

	return dms_bh_nread(orig->cow->bdev, page_idx);
}


/*
 * get Index1 plot BH with ref count increased without reading
 *
 * called in non-job-thread-context
 *
 * return bh, with reference count increased
 * never return error
 */
dms_bh_t* dms_bh_index1_nread(struct dm_dev *dd_cow, int slot_id, int page_idx)
{
	dms_bh_t *bh;

	page_idx += INDEX1_ZONE_P + INDEX1_SLOT_SIZE * slot_id;
	bh = dms_bh_get(dd_cow->bdev, page_idx);

	return bh;	
}


/*
 * get Vtree plot BH with ref count increased without reading
 *
 * called in non-job-thread-context
 *
 * return bh, with reference count increased
 * never return error
 */
dms_bh_t* dms_bh_vtree_nread(dms_bdev_t *bdev_cow, int slot_id, int page_idx)
{
	dms_bh_t *bh;

	page_idx += VERSION_TREE_ZONE_P + VTREE_SLOT_SIZE * slot_id;
	bh = dms_bh_get(bdev_cow, page_idx);

	return bh;
}


/****************
 *
 * chunk copy
 *
 ****************/


dms_bio_t* dms_chunk_copy_loop_alloc_bio(int chunk_size)
{
	int count = 0;
	dms_bio_t *bio;

	while (1) {
		count++;
		bio = bio_alloc(GFP_NOIO, chunk_size);
		if (bio) break;
		if ((count & 1023) == 0)
			DMERR("tried alloc bio %u times", count);
	}

	return bio;
}


void dms_chunk_copy_free_bio(dms_chunk_copy_t *chunk_copy)
{
	int i;

	if (!chunk_copy->bio) return;

	for (i = 0; i < chunk_copy->bio->bi_vcnt; i++)
		__free_page(chunk_copy->bio->bi_io_vec[i].bv_page);

	bio_put(chunk_copy->bio);

	memset(chunk_copy, 0, sizeof(dms_chunk_copy_t));
}


void dms_chunk_copy_bio_end(struct bio *bio, int err)
{
	dms_job_t *job;

	job = (dms_job_t*)bio->bi_private;

	if (err == -EOPNOTSUPP)
		set_bit(BIO_EOPNOTSUPP, &bio->bi_flags);

	if (err) job->error = 1;
	
	dms_job_dec_and_wake(job);
}


/*
 * return the chunk size in sector of @chunk on @bdev
 *
 * the last chunk of orig might be less than a chunk
 */
uint32 dms_chunk_copy_get_chunk_size(dms_orig_t *orig, dms_bdev_t *bdev, chunk_t chunk)
{
	uint64 shift = orig->cow->sector_shift;
	uint64 bdev_size = dms_bdev_size_sect(bdev);

	// last chunk?
	if (chunk == (bdev_size >> shift))
		return RIGHTBITS64(bdev_size, shift);
	else
		return 1 << shift;
}


/*
 * return the number of sectors will be copied
 */
uint32 dms_chunk_copy_calc_sect(
	dms_orig_t *orig,
	dms_bdev_t *bdev_d, chunk_t chunk_d,
	dms_bdev_t *bdev_s, chunk_t chunk_s)
{
	return min(
		dms_chunk_copy_get_chunk_size(orig, bdev_d, chunk_d),
		dms_chunk_copy_get_chunk_size(orig, bdev_s, chunk_s)
	);
}


/*
 * alloc a bio and corresponding pages
 */
void dms_chunk_copy_alloc_bio(
	dms_orig_t *orig, dms_chunk_copy_t *chunk_copy,
	dms_bdev_t *bdev_d, chunk_t chunk_d,
	dms_bdev_t *bdev_s, chunk_t chunk_s)
{
	int i;
	dms_bio_t *bio;
	int copy_size;  // in byte
	uint32 chunk_size;

	chunk_size = orig->cow->misc.disk->chunk_size;

	copy_size = dms_chunk_copy_calc_sect(orig, bdev_d, chunk_d, bdev_s, chunk_s) << 9;

	bio = dms_chunk_copy_loop_alloc_bio(chunk_size);

	bio->bi_sector = (uint64)(chunk_s) << orig->cow->sector_shift;
	bio->bi_bdev = bdev_s;
	bio->bi_end_io = dms_chunk_copy_bio_end;
	bio->bi_size = copy_size;
	bio->bi_private = g_job;

	bio->bi_vcnt = 0;

	for (i = 0; i < chunk_size; i++) {
		bio->bi_vcnt++;
		
		bio->bi_io_vec[i].bv_page = dms_page_loop_alloc();
		bio->bi_io_vec[i].bv_offset = 0;
				
		bio->bi_io_vec[i].bv_len = (copy_size > PAGE_SIZE) ? PAGE_SIZE : copy_size;

		copy_size -= PAGE_SIZE;
		if (unlikely(copy_size <= 0)) break;
	}

	chunk_copy->bio = bio;
	chunk_copy->bdev_s = bdev_s;
	chunk_copy->chunk_s = chunk_s;
	chunk_copy->status = DMS_CC_TO_WRITE;
}


int dms_chunk_copy_source_changed(dms_chunk_copy_t *chunk_copy, dms_bdev_t *bdev_s, chunk_t chunk_s)
{
	int r;

	r = (chunk_copy->chunk_s!= chunk_s) ||(chunk_copy->bdev_s != bdev_s);
	r = !!r;

	return r;
}


int dms_chunk_copy_dest_changed(dms_chunk_copy_t *chunk_copy, dms_bdev_t *bdev_d, chunk_t chunk_d)
{
	int r;

	r = (chunk_copy->chunk_d != chunk_d) ||(chunk_copy->bdev_d != bdev_d);
	r = !!r;

	return r;
}


/*
 * this function is highly depend on the implement of bio_alloc() in kernel
 */
void dms_chunk_copy_reinit_bio(
	dms_orig_t *orig, dms_chunk_copy_t *chunk_copy, 
	dms_bdev_t *bdev_d, chunk_t chunk_d)
{
	int i;
	dms_bio_t *bio = chunk_copy->bio;

	bio->bi_sector = (uint64)(chunk_d) <<  orig->cow->sector_shift;
	bio->bi_bdev = bdev_d;
	bio->bi_phys_segments = 0;
	bio->bi_next = 0;

	bio->bi_flags = ((bio->bi_flags >> BIO_POOL_OFFSET) << BIO_POOL_OFFSET) | (1 << BIO_UPTODATE);

	bio->bi_size = 0;
	for (i = 0; i < bio->bi_vcnt; i++)
		bio->bi_size += bio->bi_io_vec[i].bv_len;

	chunk_copy->bdev_d = bdev_d;
	chunk_copy->chunk_d = chunk_d;
	chunk_copy->status = DMS_CC_DONE;
}


void dms_chunk_copy_submit_bio(int rw, dms_bio_t *bio)
{
	atomic_inc(&g_job->io_count);
	submit_bio(rw, bio);
}


/*
 * copy chunk from @xxx_s to @xxx_d
 *
 * if IO error while coping, set the g_job->error to 1
 *
 * source holding retry:
 *   chunk_copy->bio
 *
 * return 0 if ok, or the given bio was already done
 * return ERETRY if reading
 */
int dms_chunk_copy(
	dms_orig_t *orig, dms_chunk_copy_t *chunk_copy,
	dms_bdev_t *bdev_d, chunk_t chunk_d,
	dms_bdev_t *bdev_s, chunk_t chunk_s)
{
	if (chunk_copy->status == DMS_CC_TO_READ) {
to_read:
		dms_chunk_copy_alloc_bio(orig, chunk_copy, bdev_d, chunk_d, bdev_s, chunk_s);
		dms_chunk_copy_submit_bio(READ, chunk_copy->bio);
		return ERETRY;
	}

	if (dms_chunk_copy_source_changed(chunk_copy, bdev_s, chunk_s)) {
		dms_chunk_copy_free_bio(chunk_copy);
		goto to_read;
	}
	
	if (chunk_copy->status == DMS_CC_TO_WRITE) {
to_write:
		dms_chunk_copy_reinit_bio(orig, chunk_copy, bdev_d, chunk_d);

		dms_chunk_copy_submit_bio(WRITE, chunk_copy->bio);

		return ERETRY;
	}

	if (dms_chunk_copy_dest_changed(chunk_copy, bdev_d, chunk_d))
		goto to_write;

	return 0;
}


/****************
 *
 * index1, index2
 *
 ****************/


/*
 * get the Index1 of @chunk_dmd of @orig
 *
 * return the index1 if ok
 * return 0 if @chunk_dmd has no index1
 */
plot_t dms_index1_get(dms_orig_t *orig, chunk_t chunk_dmd)
{
	plot_t index1;

	index1 = INDEX1_DISK(chunk_dmd);

	return index1;
}


/*
 */
void dms_index1_set(dms_orig_t *orig, chunk_t chunk_dmd, plot_t index1)
{
	INDEX1_DISK(chunk_dmd) = index1;
	dms_bh_awrite(orig->cow, INDEX1_BH(chunk_dmd));
}


/*
 * set index2
 *
 * the caller shoud have read  AHEAD the index2 plot already
 *
 * return 0 if this index2 plot is empty after set
 * return 1 if this index2 plot is not empty after set
 */
int dms_index2_set(
	dms_orig_t *orig, 
	chunk_t chunk_dmd, 
	plot_t index2_plot_nr, 
	plot_t elist_plot_nr)
{
	int i;
	plot_t *p;
	dms_bh_t *index2_plot_bh;

	index2_plot_bh = dms_bh_plot_aread(orig, index2_plot_nr);
	BUG_ON(IS_RETRY(index2_plot_bh));

	p = page_address(index2_plot_bh->b_page);

	p[CHUNK_2_INDEX2(chunk_dmd)] = elist_plot_nr;

	dms_bh_awrite(orig->cow, index2_plot_bh);

	if (elist_plot_nr) return 1;

	for (i = 0; i < 1024; i++)
		if (p[i]) return 1;

	return 0;
}


/*
 * return the index2 plot number if ok
 * return 0 if no index2 plot
 * return ERETRY if reading
 */
plot_t dms_index2_get(dms_orig_t *orig, chunk_t chunk_dmd, plot_t index1)
{
	plot_t *p;
	plot_t index2;
	dms_bh_t *bh;

	bh = dms_bh_plot_aread(orig, index1);
	RET_RETRY(bh);

	p = page_address(bh->b_page);
	index2 = p[CHUNK_2_INDEX2(chunk_dmd)];

	return index2;
}


/****************
 *
 * chunk
 *
 ****************/


/*
 * return the number of unused chunks
 *
 * return 0 means plot or chunk zone cann't expand any more
 */
uint32 dms_chunk_unused_nr(dms_cow_t *cow)
{
	dms_misc_disk_t *misc_disk = cow->misc.disk;
	
	return misc_disk->chunk_zone_start - misc_disk->plot_zone_end - 1;
}


/*
 * return the real number of unused chunks
 *
 * return 0 means cow is full
 */
uint32 dms_chunk_real_unused_nr(dms_cow_t *cow)
{
	dms_misc_disk_t *misc_disk = cow->misc.disk;
	
	return misc_disk->cow_size_in_chunk - misc_disk->plot_zone_end - misc_disk->chunk_zone_used;
}


/*
 * expand chunk zone
 *
 * return 0 if ok
 * return ENOSPC if cow full
 */
int dms_chunk_zone_expand(dms_cow_t *cow)
{
	int expand, nr_chunk;
	static uint32 c = 0;

	nr_chunk = dms_chunk_unused_nr(cow);
	if (!nr_chunk) {
		if ((c++ & 4095) == 0)
			DMERR("no more chunks in %s", dms_bdev_name(cow->bdev));
		return -ENOSPC;
	} 

	expand = (nr_chunk >= 4096) ? 4096 : nr_chunk;
	cow->misc.disk->chunk_zone_start -= expand;
	dms_bh_awrite(cow, cow->misc.bh);

	return 0;
}


/*
 * return chunk bitmap of @chunk_nr (1 bit)
 */
uint32 dms_chunk_bitmap_get(dms_cow_t *cow, chunk_t chunk_nr)
{
	void *p;
	int page_idx, bitmap_idx;

	page_idx = CHUNK_2_BMP_PAGE_IDX(chunk_nr);
	bitmap_idx = CHUNK_2_BMP_IDX(chunk_nr);

	p = (void*) cow->chunk_bitmap.disk[page_idx];

	return dms_read_bit(p, bitmap_idx);
}


/*
 * write chunk bitmap of @chunk_nr (1 bit)
 */
void  dms_chunk_bitmap_set(dms_cow_t *cow, chunk_t chunk_nr, int bit)
{
	void *p;
	int page_idx, bitmap_idx;

	page_idx = CHUNK_2_BMP_PAGE_IDX(chunk_nr);
	bitmap_idx = CHUNK_2_BMP_IDX(chunk_nr);

	p = (void*) cow->chunk_bitmap.disk[page_idx];

	dms_write_bit(p, bitmap_idx, bit);

	dms_bh_awrite(cow, cow->chunk_bitmap.bh[page_idx]);
}


/*
 * alloc a cow chunk
 *
 * return chunk number if ok
 * return EXX if error
 */
chunk_t dms_chunk_alloc(dms_cow_t *cow)
{
	uint32 r;
	chunk_t chunk_cow;

	chunk_cow = cow->next_alloc_chunk;

	while (1) {
		// expand chunk zone?
		if (chunk_cow < cow->misc.disk->chunk_zone_start) {
			r = dms_chunk_zone_expand(cow);
			RET_EXX(r);
		}

		if (!dms_chunk_bitmap_get(cow, chunk_cow)) {
			dms_chunk_bitmap_set(cow, chunk_cow, 1);
			cow->next_alloc_chunk = chunk_cow - 1;

			cow->misc.disk->chunk_zone_used++;
			dms_bh_awrite(cow, cow->misc.bh);

			return chunk_cow;
		}
		
		chunk_cow--;
	}
}


void dms_chunk_free(dms_cow_t *cow, chunk_t chunk_cow)
{
	if (chunk_cow > cow->next_alloc_chunk)
		cow->next_alloc_chunk = chunk_cow;

	cow->misc.disk->chunk_zone_used--;
	dms_bh_awrite(cow, cow->misc.bh);	

	dms_chunk_bitmap_set(cow, chunk_cow, 0);
}


/****************
 *
 * plot
 *
 ****************/


/*
 * expand plot zone one more chunk bigger
 *
 * return 0 if ok
 * return ENOSPC if cow full
 */
int dms_plot_zone_expand(dms_cow_t *cow)
{
	int nr_chunk;
	static uint32 c = 0;
	
	nr_chunk = dms_chunk_unused_nr(cow);
	if (!nr_chunk) {
		if ((c++ & 4095) == 0)
			DMERR("no more plots in %s", dms_bdev_name(cow->bdev));
		return -ENOSPC;
	}

	nr_chunk >>= 2;
	if (nr_chunk > 64) nr_chunk = 64;
	if (nr_chunk == 0) nr_chunk = 1;
	cow->misc.disk->plot_zone_end += nr_chunk;
	dms_bh_awrite(cow, cow->misc.bh);
	
	return 0;
}


int dms_plot_beyond(dms_cow_t *cow, uint32 plot_nr)
{
	int r;

	r =(plot_nr + PLOT_ZONE_P) / cow->misc.disk->chunk_size > cow->misc.disk->plot_zone_end;

	return r;
}


/*
 * return plot bitmap of @plot_nr (2 bits)
 * never return error
 */
int dms_plot_bitmap_get(dms_cow_t *cow, plot_t plot_nr)
{
	void *p;
	int page_idx, bitmap_idx;

	page_idx = PLOT_2_BMP_PAGE_IDX(plot_nr);
	bitmap_idx = PLOT_2_BMP_IDX(plot_nr);

	p = (void*) cow->plot_bitmap.disk[page_idx];

	return dms_read_2bits(p, bitmap_idx);
}


/*
 * write plot bitmap of @plot_nr (2 bits)
 */
void dms_plot_bitmap_set(dms_cow_t *cow, plot_t plot_nr, int bits)
{
	void *p;
	int page_idx, bitmap_idx;

	page_idx = PLOT_2_BMP_PAGE_IDX(plot_nr);
	bitmap_idx = PLOT_2_BMP_IDX(plot_nr);

	p = cow->plot_bitmap.disk[page_idx];

	dms_write_2bits(p, bitmap_idx, bits);

	dms_bh_awrite(cow, cow->plot_bitmap.bh[page_idx]);
}


/*
 * alloc a new elist plot, which bmp is less than @req_bmp.
 * read AHEAD the plot, make index2 point to it.
 * make sure there is no CO equal to @chunk_dmd in new elist plot
 * increase the ref count of elist plot bh.
 * if plot allocated, set the bitmap to 01(PLOT_BMP_USED)
 *
 * return elist plot nr if ok
 * return ERETRY if reading
 * return EXX if error
 */
plot_t dms_plot_elist_alloc(
	dms_orig_t *orig, 
	chunk_t chunk_dmd, 
	plot_t index1, 
	uint32 req_bmp, 
	dms_bh_t **bh_ret)
{
	uint32 r;
	int bmp;
	plot_t plot_nr;
	dms_bh_t *bh_plot;
	dms_cow_t *cow;

	plot_nr = 0;
	cow = orig->cow;

retry:
	do {
		plot_nr++;

		if (dms_plot_beyond(cow, plot_nr)) {
			r = dms_plot_zone_expand(cow);
			RET_EXX(r);
		}

		bmp = dms_plot_bitmap_get(cow, plot_nr);
	} while (bmp > req_bmp);

	if (bmp == 0) {
		bh_plot = dms_bh_plot_nread(orig, plot_nr);
		dms_zero_page(bh_plot->b_page);
	} else {
		bh_plot = dms_bh_plot_aread(orig, plot_nr);
		RET_RETRY(bh_plot);

		if (dms_elist_find_co(bh_plot, chunk_dmd))
			goto retry;
	}

	if (bh_ret) *bh_ret = bh_plot;

	if (bmp == PLOT_BMP_EMPTY)
		dms_plot_bitmap_set(cow, plot_nr, PLOT_BMP_USED);

	dms_index2_set(orig, chunk_dmd, index1, plot_nr);
	
	return plot_nr;
}


/*
 * alloc a index2 plot and an elist plot. read AHEAD them.
 * the bmp of elist plot is less than 01(PLOT_BMP_USED)
 * make sure there is no CO equal to @chunk_dmd in new elist plot
 * increase the ref count of bhs
 * set the index2 plot bmp to 11(PLOT_BMP_FULL)
 * set the elist plot bmp to 01(PLOT_BMP_USED)
 *
 * the plot numbers are return by @plot_nr_index2 and @plot_nr_elist
 * the bh of elist plot is returned by @bh_ret_list
 *
 * return 0 if ok
 * return ERETRY if reading
 * return EXX if error
 */
int dms_plot_index2_elist_alloc(
	dms_orig_t *orig, 	plot_t *plot_nr_index2,
	plot_t *plot_nr_elist, dms_bh_t **bh_ret_elist, chunk_t chunk_dmd)
{
	uint32 r;
	int bmp, bmp_elist = 0;
	plot_t plot_nr;
	dms_bh_t *bh_index2_plot = 0, *bh_elist_plot = 0;
	dms_cow_t *cow;

	plot_nr = 0;
	*plot_nr_index2 = 0;
	*plot_nr_elist = 0;
	cow = orig->cow;

	do {
		plot_nr++;

		if (dms_plot_beyond(cow, plot_nr)) {
			r = dms_plot_zone_expand(cow);
			RET_EXX(r);
		}

		bmp = dms_plot_bitmap_get(cow, plot_nr);

		 if (bmp <= PLOT_BMP_USED && *plot_nr_elist == 0) {
			bmp_elist = bmp;
			*plot_nr_elist = plot_nr;

			if (!bmp) {
				bh_elist_plot = dms_bh_plot_nread(orig, plot_nr);
				dms_zero_page(bh_elist_plot->b_page);
			} else {
				bh_elist_plot = dms_bh_plot_aread(orig, plot_nr);
				RET_RETRY(bh_elist_plot);

				if (dms_elist_find_co(bh_elist_plot, chunk_dmd))
					*plot_nr_elist = 0;
			}
		} else if (bmp == PLOT_BMP_EMPTY && *plot_nr_index2 == 0) {
			*plot_nr_index2 = plot_nr;
			
			bh_index2_plot = dms_bh_plot_nread(orig, plot_nr);
			dms_zero_page(bh_index2_plot->b_page);
		}
	} while (*plot_nr_index2 == 0 || *plot_nr_elist == 0);


	dms_plot_bitmap_set(cow, *plot_nr_index2, PLOT_BMP_FULL);

	if (bh_ret_elist) *bh_ret_elist = bh_elist_plot;

	if (bmp_elist == PLOT_BMP_EMPTY)
		dms_plot_bitmap_set(cow, *plot_nr_elist, PLOT_BMP_USED);

	return 0;
}


/*
 * find dm_dev with @tag by dm_dev
 *
 * return the device if found
 * return 0 if not
 */
dms_dm_dev_t* dms_dmd_find_by_dd(
	struct dm_dev *dd_orig, 
	struct dm_dev *dd_cow, 
	int tag)
{
	dms_dm_dev_t *dmd;

	list_for_each_entry(dmd, &dms_dmds_lh, dmds_ln) {
		if ((dmd->tag == tag) && (dmd->orig->bdev == dd_orig->bdev)
			&& (dmd->orig->cow->bdev == dd_cow->bdev)) {
			return dmd;
		}
	}
	return 0;
}


/*
 * find dm_dev with @tag by orig
 *
 * return the device if found
 * return 0 if not
 */
dms_dm_dev_t* dms_dmd_find_by_orig(dms_orig_t *orig, int tag)
{
	dms_dm_dev_t *dmd;

	list_for_each_entry(dmd, &dms_dmds_lh, dmds_ln) {
		if ((dmd->tag == tag) && (dmd->orig == orig))
			return dmd;
	}
	return 0;
}


/****************
 *
 * vtree
 *
 ****************/


/*
 * print vtree recursively
 */
void dms_vtree_do_print(int level, dms_orig_t *orig, uint16 ver)
{
	int i;
	static char str[10240];
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	str[0] = 0;

	// print verion number
	sprintf(str, "(ver-%04hu)  ", ver);

	// print indent space
	for (i = 0; i < level - 1; i++)
		strcat(str, "  ");
	
	if (level)
		strcat(str, "+-");

	// name or tag
	if (VTREE_DISK(ver).tag == 0)
		sprintf(str + strlen(str), "ORIGIN");
	else if (VTREE_DISK(ver).tag == VTREE_TAG_GHOST)
		sprintf(str + strlen(str), "GHOST");
	else
		sprintf(str + strlen(str), "%04hu",  VTREE_DISK(ver).tag);

	if (VTREE_DISK(ver).tag) {
		// nr of exceptions
		sprintf(str + strlen(str), " - %u",VTREE_DISK(ver).nr_exceptions);
		// birthday
		sprintf(str + strlen(str), " - %u",VTREE_DISK(ver).birthday);
	}

	DMINFO("%s", str);

	// print children
	for (ver = aux[ver].child; ver; ver = aux[ver].brother)
		dms_vtree_do_print(level + 1, orig, ver);
}


int dms_vtree_print(dms_orig_t *orig)
{
	int i;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	LOCK_ORIG();

	DMINFO("############################");

	// print vtree disk in array style
	for (i = 0; i < MAX_NR_OF_VERSIONS; i++)
		if (VTREE_DISK(i).tag || VTREE_DISK(i).father)
			DMINFO("ver=%d father=%d tag=%d birthday=%u nr_e=%d child_nr=%d child=%d brother=%d",
				i, VTREE_DISK(i).father, VTREE_DISK(i).tag, VTREE_DISK(i).birthday, 
				VTREE_DISK(i).nr_exceptions, aux[i].nr_children, aux[i].child, aux[i].brother);

	// print in tree style
	dms_vtree_do_print(0, orig, 0);

	// print Deleting versions
	for (i = 0; i < MAX_NR_OF_VERSIONS; i++)
		if (VTREE_DISK(i).tag == VTREE_TAG_DELETING)
			DMINFO("(ver-%04hu)  +-DELETING - %u", i, VTREE_DISK(i).nr_exceptions);

	UNLOCK_ORIG();

	DMINFO("----------------------------");

	return 0;
}



/*
 * return the number of used versions
 */
int dms_vtree_used_versions(dms_orig_t *orig)
{
	uint16 ver;
	int total = 1;

	for (ver = 1; ver < MAX_NR_OF_VERSIONS; ver++)
		if (VTREE_DISK(ver).tag)
			total++;

	return total;	
}


/*
  * check whether @ver_nr of @orig is ghost version
  *
  * return 1 if @ver_nr is ghost
  * otherwise return 0
  *
  * @ver_nr == 0 is not allowed
  */
int dms_vtree_is_ghost(dms_orig_t *orig, uint16 ver_nr)
{
	BUG_ON(!ver_nr);
	
	return VTREE_DISK(ver_nr).tag == VTREE_TAG_GHOST;	
}


int dms_vtree_is_deleting(dms_orig_t *orig, uint16 ver_nr)
{
	BUG_ON(!ver_nr);
	
	return VTREE_DISK(ver_nr).tag == VTREE_TAG_DELETING;
}


int dms_vtree_is_live(dms_orig_t *orig, uint16 ver_nr)
{
	return (VTREE_DISK(ver_nr).tag) && (VTREE_DISK(ver_nr).tag <= VTREE_TAG_LIVE);
}


/*
 * return the number of children of @ver_nr
 */
inline int dms_vtree_has_child(dms_orig_t *orig, uint16 ver_nr)
{
	return orig->vtree.auxiliary[ver_nr].nr_children;
}


/*
 * add @child_ver as child of @father_ver in @aux array
 */
void dms_vtree_aux_add_child(dms_vtree_aux_t *aux, uint16 father_ver, uint16 child_ver)
{
	int i;
	
	aux[father_ver].nr_children++;
	if (aux[father_ver].child) {  
		// insert to brother list
		for (i = aux[father_ver].child; aux[i].brother; i = aux[i].brother);
		aux[i].brother = child_ver;
	} else { 
		// insert to child link
		aux[father_ver].child = child_ver;
	}
}


/*
 * init vtree auxiliary array according ondisk data
 */
void dms_vtree_init_auxiliary(dms_orig_t *orig)
{
	uint16 ver;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;
	memset(aux, 0, sizeof(orig->vtree.auxiliary));

	for (ver = 1; ver < MAX_NR_OF_VERSIONS; ver++) 
		if (dms_vtree_is_live(orig, ver))
			dms_vtree_aux_add_child(aux, VTREE_DISK(ver).father, ver);
}


/*
 * set the tag of @ver_nr as @tag
 */
void dms_vtree_set_tag(dms_orig_t *orig, unsigned short ver_nr, int tag)
{
	VTREE_DISK(ver_nr).tag = tag;
	dms_vtree_init_auxiliary(orig);
	dms_bh_awrite(orig->cow, VTREE_BH(ver_nr));
}


/*
 * set the tag of @ver_nr as Deleting
 *
 * note: Deleting version has orig as father, has no children, not exist in Aux array
 * the caller should move all it's children away first
 */
void dms_vtree_set_deleting(dms_orig_t *orig, unsigned short ver_nr)
{
	VTREE_DISK(ver_nr).father = 0;
	dms_vtree_set_tag(orig, ver_nr, VTREE_TAG_DELETING);
	DMDEBUG("(V%u,%s,%s) was marked as deleting", 
		ver_nr, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
	);
}


/*
 * return the version number of the first child of @ver_nr
 */
int dms_vtree_1st_child(uint16 ver_nr, dms_orig_t *orig)
{
	return orig->vtree.auxiliary[ver_nr].child;
}


/*
 * return the father version of the @ver_nr
 * for orig version, return 0
 */
int dms_vtree_father(dms_orig_t *orig, uint16 ver)
{
	return VTREE_DISK(ver).father;
}


/*
 * move all children of @old_father_ver_nr under @new_father_ver_nr
 */
void dms_vtree_move_children(dms_orig_t *orig, uint16 new_father_ver_nr, uint16 old_father_ver_nr)
{
	int i;

	for (i = 1; i < MAX_NR_OF_VERSIONS; i++)
		if (VTREE_DISK(i).father == old_father_ver_nr) {
			VTREE_DISK(i).father = new_father_ver_nr;
			dms_bh_awrite(orig->cow, VTREE_BH(i));
		}

	dms_vtree_init_auxiliary(orig);
}


/*
 * return version number if found
 * returen 0 if not found
 */
uint16 dms_vtree_get_version_by_tag(dms_orig_t *orig, uint16 tag)
{
	int ver;
	dms_vtree_aux_t *aux;
	
	aux = orig->vtree.auxiliary;

	if (tag > VTREE_TAG_USER) return 0;

	for (ver = 0; ver < MAX_NR_OF_VERSIONS; ver++)
		if (VTREE_DISK(ver).tag == tag) 
			return ver;

	return 0;
}


/*
 * find out an unused tag
 *
 * It is impossible that no tag is available
 *
 * return the tag
 */
uint16 dms_vtree_find_unused_tag(dms_orig_t *orig)
{
	uint16 ver_nr, tag;


	// find out the max tag being used
	tag = 0;
	for (ver_nr = 1; ver_nr < MAX_NR_OF_VERSIONS; ver_nr++)
		if (VTREE_DISK(ver_nr).tag <= VTREE_TAG_USER)
			tag = max(tag, VTREE_DISK(ver_nr).tag);

	// increase max tag untill it's not being used
	while (1) {
		tag++;
		if (tag > VTREE_TAG_USER) tag = 1;
		if (!dms_vtree_get_version_by_tag(orig, tag)) 
			return tag;
	}
}


/* find an unused version
 *
 * return version number if ok
 * return 0 if not found
 */
int dms_vtree_find_unused_ver(dms_orig_t *orig)
{
	uint16 ver;
	static uint32 c = 0;

	for (ver = 1; ver < MAX_NR_OF_VERSIONS; ver++)
		if (!VTREE_DISK(ver).tag) return ver;

	c++;
	if (c < 0xff || (c & 0xff) == 0) {
		DMERR("no version available in (%s,%s)", 
			dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
		);
	}

	return 0;
}


/*
 * insrert a new child between v0 and child(v0)
 *
 * NOTE: only can be call in dms_message()
 *
 * return the tag of new child if ok
 * return EXX if error
 */
int dms_vtree_snap_orig(dms_orig_t *orig, char *memo)
{
	int r;
	dms_vtree_aux_t *aux;
	uint16 new_child_tag;
	uint16 new_child_ver, old_child_ver;

	aux = orig->vtree.auxiliary;

	LOCK_ORIG();

	// find a new version nr
	new_child_ver = dms_vtree_find_unused_ver(orig);
	if (!new_child_ver) {
		r = -ENOSPC;
		goto out;
	}

	// find a new tag
	new_child_tag = dms_vtree_find_unused_tag(orig);

	old_child_ver = aux[0].child;

	// insert the new child
	VTREE_DISK(new_child_ver).father = 0;
	VTREE_DISK(new_child_ver).tag = new_child_tag;
	VTREE_DISK(new_child_ver).nr_exceptions = 0;
	VTREE_DISK(new_child_ver).birthday = dms_get_time();
	if (memo) strncpy(VTREE_DISK(new_child_ver).memo, memo, VTREE_MEMO_LEN);

	r = dms_bh_swrite(VTREE_BH(new_child_ver));
	if (r) {
		dms_io_err(orig->cow->bdev, "vtree");
		goto out;
	}

	if (old_child_ver) {
		VTREE_DISK(old_child_ver).father = new_child_ver;

		r = dms_bh_swrite(VTREE_BH(old_child_ver));
		if (r) {
			dms_io_err(orig->cow->bdev, "vtree");
			goto out;
		}
	}

	dms_vtree_init_auxiliary(orig);

	r = new_child_tag;

out :
	UNLOCK_ORIG();
	return r;
}


/*
 * NO disk writeback. I am both job-thread and non-job-thread.
 * the caller should writeback VTREE_BH
 */
void dms_vtree_add_child(
	dms_orig_t *orig, 
	uint16 father_ver, 
	uint16 child_ver, 
	uint16 child_tag, 
	char *memo)
{
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;
	
	VTREE_DISK(child_ver).father = father_ver;
	VTREE_DISK(child_ver).tag = child_tag;
	VTREE_DISK(child_ver).birthday = dms_get_time();
	if (memo) strncpy(VTREE_DISK(child_ver).memo, memo, VTREE_MEMO_LEN);

	dms_vtree_init_auxiliary(orig);
}


/*
 * make @ver_nr of @orig be ghost, by chaging it's tag to VTREE_TAG_GHOST
 *
 * NO disk writeback. I am both job-thread and non-job-thread.
 * the caller should writeback VTREE_BH
 */
void dms_vtree_ghost(dms_orig_t *orig, uint16 ver_nr)
{
	VTREE_DISK(ver_nr).tag = VTREE_TAG_GHOST;
	VTREE_DISK(ver_nr).memo[0] = 0;
	dms_job_plug(orig->cow, &(orig->clean_info.job));
	dms_job_wake_up(orig->cow);

	DMDEBUG("(V%u,%s,%s) was marked as ghost", 
		ver_nr, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
	);
}


/*
 * copy version info(tag, birthday, memo) from @ver_nr_s to @@ver_nr_d
 */
void dms_vtree_copy_info(dms_orig_t *orig, uint16 ver_nr_d, uint16 ver_nr_s)
{
	VTREE_DISK(ver_nr_d).tag = VTREE_DISK(ver_nr_s).tag;
	VTREE_DISK(ver_nr_d).birthday = VTREE_DISK(ver_nr_s).birthday;
	strncpy(VTREE_DISK(ver_nr_d).memo, VTREE_DISK(ver_nr_s).memo, VTREE_MEMO_LEN);
}


/*
 * ghost @ver_nr, add a new version with the tag of @ver_nr as the child of @ver_nr
 *
 *            freeze(T1)
 * V0       -------------->  V0
 *  +-V1(T1)                  +-V1(Ghost)
 *     +-V2(T2)                  +-V2(T2)
 *                                  +-V3(T1)
 *
 *
 * return new version if ok
 * return ENOSPC if no version available
 */
uint16 dms_vtree_relocate_tag(dms_orig_t *orig, uint16 ver_nr)
{
	uint16 new_ver;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	BUG_ON (!ver_nr);
	
	// it's alread a ghost 
	BUG_ON(dms_vtree_is_ghost(orig, ver_nr));

	new_ver = dms_vtree_find_unused_ver(orig);
	if (!new_ver) goto bad;

	dms_vtree_add_child(orig, ver_nr, new_ver, VTREE_DISK(ver_nr).tag, VTREE_DISK(ver_nr).memo);
	
	DMDEBUG("(%u,%s,%s) moved from V%u to V%u", 
		VTREE_DISK(ver_nr).tag, dms_bdev_name(orig->bdev), 
		dms_bdev_name(orig->cow->bdev), ver_nr, new_ver
	);

	dms_vtree_ghost(orig, ver_nr);

	dms_bh_awrite(orig->cow, VTREE_BH(new_ver));
	if (VTREE_BH(new_ver) != VTREE_BH(ver_nr))
		dms_bh_awrite(orig->cow, VTREE_BH(ver_nr));

	return new_ver;
bad:
	return -ENOSPC;
}


/*
 * delete the Deleting version @ver_nr
 */
void dms_vtree_delete_deleting_version(dms_orig_t *orig, uint16 ver_nr)
{
	BUG_ON(VTREE_DISK(ver_nr).nr_exceptions);
	BUG_ON(!dms_vtree_is_deleting(orig, ver_nr));

	dms_vtree_set_tag(orig, ver_nr, 0);

	DMDEBUG("(V%u,%s,%s) was deleted", 
		ver_nr, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
	);
}


/*
 * chunk operation 1?
 * refer to the comments above vtree_clean()
 *
 * return 1 if yes, 0 no
 */
int dms_vtree_trigger_delete(dms_orig_t *orig, uint16 ver_nr)
{
	return dms_vtree_is_deleting(orig, ver_nr) && 
		(VTREE_DISK(ver_nr).nr_exceptions >= 1);
}


/*
 * chunk operation 2?
 * refer to the comments above vtree_clean()
 *
 * return 1 if yes, 0 no
 */
int dms_vtree_trigger_merge(dms_orig_t *orig, uint16 ver)
{
	uint16 ver_child;

	ver_child = dms_vtree_1st_child(ver, orig);
	return dms_vtree_is_ghost(orig, ver) && 
		(dms_vtree_has_child(orig, ver) == 1) && 
		(VTREE_DISK(ver).nr_exceptions >= 1) &&
		(VTREE_DISK(ver_child).nr_exceptions >= 1);
}


/*
 * chunk operation 3?
 * refer to the comments above vtree_clean()
 *
 * return 1 if yes, 0 no
 */
int dms_vtree_trigger_merge_2(dms_orig_t *orig, uint16 ver_nr)
{
	uint16 child_ver_nr;

	child_ver_nr = dms_vtree_1st_child(ver_nr, orig);
	return dms_vtree_is_ghost(orig, ver_nr) && 
		(dms_vtree_has_child(orig, ver_nr) > 1) && 
		(VTREE_DISK(ver_nr).nr_exceptions >= 1);
}


/*
 * *: with 0+ exceptions
 * +: with 1+ exceptions
 * -: with 0  exception
 *
 * x+: x or more than x
 *
 * G:Ghost D:Deleting L:Live
 *
 *
 * vtree operations
 * ================
 *       1              
 *   G* ---> D*         
 *                      
 *       2              
 *   D- ---> Nothing    
 *                      
 *       3               
 *   G+ ---> +-L+       
 *   +-L-    | +-..     
 *     +-..  +-D-       
 *                      
 *       4               
 *   G- ---> +-D-         
 *   +-L*    +-L*       
 *   +-..    +-..       
 *                      
 * 1. Ghost, no children:
 *      a. change Ghost to Deleting
 *
 * 2. Deleting, no exceptions:
 *      a. delete Deleting
 *
 * 3. Ghost, 1+ exceptions, 1 Live child who has no exceptions:
 *      a. move children of Live under Ghost
 *      b. set Ghost's tag to Live's tag
 *      c. set Live as Deleting
 *
 * 4. Ghost, 1+ children, no exceptions, father is NOT orig
 *      b. move all children of Ghost under the father of the Ghost
 *      a. change Ghost to Deleting
 *
 * 
 * chunk operations
 * ================
 * 
 *    +----------+
 *    |        1 |
 *    +--> D+ ---+--> D-
 * 
 *    +----------+          
 *    |        2 |
 *    +--> G+ ---+--> G-
 *         +-L+       +-L+
 *  
 *    +----------+          
 *    |        3 |           
 *    +--> G+ ---+--> G-
 *         +-L*       +-L*
 *         +-..       +-..
 *         +-L*       +-L*
 *                          
 * 1. Deleting, 1+ excpetions:
 *      a. delete exception of Deleting chunk
 * 
 * 2. Ghost, 1+ exceptions, 1 child who has 1+ exceptions:
 *      a. if the chunk of the child has exception, delete the
 *         exception of Ghost.
 *      b. otherwise, move the exception to the child
 *
 * 3. Ghost, 1+ exceptions, 1+ children:
 *      a. if all children of Ghost have exceptions, delete the
 *         exception of Ghost
 *      b. if ONLY 1 child has no exception, move the exception of
 *         Ghost to the child
 *
 * note: 
 *      a. only the 1st and 2nd operations will trigger the iteration of chunk by chunk
 *      b. operation 3 will be executed in passing just after operation 1 or 2
 *         has been executed
 *      c. Deleting Version has no children, has orig as father, not exists
 *         in Auxiliary array
 * 
 *
 * clean vtree
 *
 * return 1 if need to trigger chunk-by-chunk cleaning
 * return 0 if not
 */
int dms_vtree_clean(dms_orig_t *orig)
{
	int tag_child;
	int ver_nr;
	int ver_nr_child;
	int ver_nr_father;
	int trigger = 0;
	dms_dm_dev_t *dmd;

retry:
	for (ver_nr = 1; ver_nr < MAX_NR_OF_VERSIONS; ver_nr++) {

		ver_nr_child = dms_vtree_1st_child(ver_nr, orig);
		tag_child = VTREE_DISK(ver_nr_child).tag;

		// trigger chunk-op 1 or 2
		if (dms_vtree_trigger_delete(orig, ver_nr) || 
			dms_vtree_trigger_merge(orig,ver_nr))
		{
			trigger = 1;
		}

		// vtree op 2
		if (dms_vtree_is_deleting(orig, ver_nr) && 
			VTREE_DISK(ver_nr).nr_exceptions == 0)
		{
			dms_vtree_delete_deleting_version(orig, ver_nr);
			goto retry;
		}

		// vtree op 1,3,4 are all agaist ghost
		if (!dms_vtree_is_ghost(orig, ver_nr)) continue;

		// vtree op 1
		if (dms_vtree_has_child(orig, ver_nr) == 0) {
			dms_vtree_set_deleting(orig, ver_nr);
			goto retry;
		}

		// vtree op 3
		if ((dms_vtree_has_child(orig, ver_nr) == 1) &&
			(VTREE_DISK(ver_nr).nr_exceptions >= 1) &&
			(VTREE_DISK(ver_nr_child).nr_exceptions == 0))
		{
			dms_vtree_move_children(orig, ver_nr, ver_nr_child);
			dms_vtree_copy_info(orig, ver_nr, ver_nr_child);

			dmd = dms_dmd_find_by_orig(orig, tag_child);
			if (dmd) atomic_set(&dmd->ver, ver_nr);
			
			dms_vtree_set_deleting(orig, ver_nr_child);

			DMDEBUG("(%u,%s,%s) moved from V%u to V%u", 
				tag_child, dms_bdev_name(orig->bdev), 
				dms_bdev_name(orig->cow->bdev), ver_nr_child, ver_nr
			);

			goto retry;
		}

		// vtree op 4
		if ((dms_vtree_has_child(orig, ver_nr) >= 1) && 
			(VTREE_DISK(ver_nr).nr_exceptions == 0) &&
			(dms_vtree_father(orig, ver_nr) != 0))
		{
			ver_nr_father = dms_vtree_father(orig, ver_nr);
			dms_vtree_move_children(orig, ver_nr_father, ver_nr);
			dms_vtree_set_deleting(orig, ver_nr);

			goto retry;
		}
	}

	return trigger;
}


/****************
 *
 * elist
 *
 ****************/


/*
 * return the count of VCS and COs
 *
 * elist layout example:
 *   n 0   co co co  ...  vc vc vc vc
 */
int dms_elist_calc_used(dms_bh_t *elist_bh)
{
	void *p;
	uint16 *n;
	dms_chunk_offset_t *cos;
	
	p = page_address(elist_bh->b_page);
	n = p;
	cos = p - 2;

	return *n + cos[*n].offset;
}


/*
 * plot bitmap:
 *  00: empty
 *  01: more co-vc insertable
 *  11: full
 */
int dms_elist_calc_bmp(dms_bh_t *elist_bh)
{
	int used;

	used = dms_elist_calc_used(elist_bh);

	if (!used)
		return PLOT_BMP_EMPTY;

	if (used >= DMS_ELIST_FULL_THRESHOLD)
		return PLOT_BMP_FULL;

	return PLOT_BMP_USED;
}


/*
 * get the chunk of vc pointed by vc_index
 */
chunk_t dms_elist_get_vc(dms_bh_t *elist_bh, int vc_index)
{
	void *p;
	dms_ver_chunk_t *vcs;
	
	p = page_address(elist_bh->b_page);
	vcs = p + 4096;
	return vcs[-vc_index].chunk;
}


/*
 * set the chunk of vc pointed by vc_index to chunk_cow
 * vc[-vc_index].chunk = chunk_cow
 * @vc_index should be positive
 *
 * elist:
 *   n                        vc_index
 *   |                        |
 *   |   1  2  3    4     3   2     1
 *   n 0 co co co  [vc4] [vc3 vc2] [vc1]
 */
void dms_elist_set_vc(dms_bh_t *elist_bh, int vc_index, chunk_t chunk_cow)
{
	void *p;
	dms_ver_chunk_t *vcs;
	
	p = page_address(elist_bh->b_page);
	vcs = p + 4096;
	vcs[-vc_index].chunk = chunk_cow;
}


/*
 * find the vc with vc.ver==@ver, in the vcs of @elist_cache
 *
 *          co_index             o2      o1
 *          |                    |       |             
 *       1  2  3           6     5   4   3     2   1
 *   n 0 co co co         [vc6] [vc5 vc4 vc3] [vc2 vc1]
 *
 *
 * return a positive vc index if found
 * return a negative vc index if not found, indicating where the new vc shoud be inserted
 *     the new vc should be inserted between vc_index and vc_index-1
 *     ex: return vc_index=-2, [vc2 vc1] -> [vc3 vcNew vc1]
 */
int dms_elist_find_vc(dms_bh_t *elist_bh, int co_index, uint16 ver)
{
	void *p;
	int o1, o2, i;
	dms_ver_chunk_t *vcs;
	dms_chunk_offset_t *cos;
	
	p = page_address(elist_bh->b_page);
	cos = p - 2;
	vcs = p + 4096;

	o1 = cos[co_index - 1].offset + 1;
	o2 = cos[co_index].offset;

	for (i = o1; i <= o2; i++) {
		if (vcs[-i].ver > ver) return -i;
		if (vcs[-i].ver == ver) return i;
	}

	return -(o2 + 1);
}


/*
 * insert a vc between @vc_index and @vc_index-1
 * the caller should make sure there is enough space for the new vc
 * note: @vc_index should be positive
 *
 */
void dms_elist_new_vc(
	dms_bh_t *elist_bh, 
	int co_index, 
	int vc_index, 
	uint16 ver, 
	chunk_t chunk_cow)
{
	int i;
	void *p;
	uint16 *n, o1;
	dms_chunk_offset_t *cos;
	dms_ver_chunk_t *vcs;
	
	p = page_address(elist_bh->b_page);
	cos = p - 2;
	vcs = p + 4096;
	n = p;

	o1 = cos[*n].offset;

	for (i = o1; i >= vc_index; i--)
		vcs[-i -1] = vcs[-i];

	vcs[-vc_index].ver = ver;
	vcs[-vc_index].chunk = chunk_cow;

 	for (i = co_index; i <= *n; i++)
		cos[i].offset++;
}


/*
 * find the co with co.chunk==@chunk_dmd
 *
 * return the index of co if found
 * return 0 if co not found
 */
int dms_elist_find_co(dms_bh_t *elist_bh, chunk_t chunk_dmd)
{
	int i;
	void *p;
	uint16 *n;
	dms_chunk_offset_t *cos;
	
	p = page_address(elist_bh->b_page);
	n = p;
	cos = p - 2;

	for (i = 1; i <= *n; i++)
		if (cos[i].chunk == chunk_dmd)
			return i;

	return 0;
}


/*
 * delete the vc, where vc.v==@ver_nr, and co.c==@chunk_dmd, if exist
 * if no vc any more, and @del_co is set, delete the co and set @co_index=0
 *
 * NO sync
 *
 *           co_index
 *           |        vcs_len   vc_index
 *           |        |         |
 * n 0 co1 co2 co3 [vc4] [vc3 vc2] [vc1]
 *
 * return the c of deleted vc
 * return 0 if not exist.
 */
chunk_t dms_elist_delete_vc(dms_bh_t *elist_bh, uint16 ver_nr, int *co_index, int del_co)
{
	int i;
	int vc_index;
	int vcs_len;
	chunk_t ret_chunk_nr;
	void *p;
	uint16 *n;
	dms_chunk_offset_t *cos;  // Chunk-Offset
	dms_ver_chunk_t *vcs;  //Ver-Chunk

	ret_chunk_nr = 0;
	p = page_address(elist_bh->b_page);
	n = p; // Number of COs
	cos = p - 2;
	vcs = p + 4096;

	vc_index = dms_elist_find_vc(elist_bh, *co_index, ver_nr);
	if (vc_index < 0) return 0;

	ret_chunk_nr = vcs[-vc_index].chunk;
	vcs_len = cos[*n].offset;

	// move vcs one right
	for (i = vc_index; i < vcs_len; i++)
		vcs[-i] = vcs[-i -1];

	// decrease co.offset
	for (i = *co_index; i <= *n; i++)
		cos[i].offset--;

	// delete co if no vcs
	if (del_co && cos[*co_index].offset == cos[(*co_index) - 1].offset) {
		for (i = *co_index; i < *n; i++)
			cos[i] = cos[i + 1];
		(*n)--;
		*co_index = 0;
	}

	return ret_chunk_nr;	
}


/*
 * move co and vcs from @elist_bh_s' to @elist_bh_d, new vc
 *
 * @co_index: which co to be moved
 * @elist_bh_d should be empty
 * 
 * elist src:
 *      
 *  n_s     co_index   o3         o2                 o1
 *  |       |          |          |                  |             
 *  |   1   2   3      8   7      6   5   4   3      2   1              
 *  n 0 co1 co2 co3   [vc8 vc7]  [vc6 vc5 vc4 vc3]  [vc2 vc1]
 *                    |-len2 -|  |---- len1 -----|
 *
 */
void dms_elist_move_co_vc(
		dms_bh_t *elist_bh_s,
		dms_bh_t *elist_bh_d,
		int co_index, chunk_t chunk_dmd)
{
	int i;
	int o1, o2, o3;
	int len1, len2;
	void *p_s, *p_d;
	uint16 *n_s, *n_d;
	dms_chunk_offset_t *cos_s, *cos_d;
	dms_ver_chunk_t *vcs_s, *vcs_d;

	p_s = page_address(elist_bh_s->b_page);
	n_s = p_s; // Number of COs
	cos_s = p_s - 2;
	vcs_s = p_s + 4096;

	p_d = page_address(elist_bh_d->b_page);
	n_d = p_d; // Number of COs
	cos_d = p_d - 2;
	vcs_d = p_d + 4096;

	// some offset
	o1 = cos_s[co_index - 1].offset;
	o2 = cos_s[co_index].offset;
	o3 = cos_s[*n_s].offset;

	// some length
	len1 = o2 - o1;
	len2 = o3 - o2;

	// dst: set the new co
	*n_d = 1;
	cos_d[1].chunk = chunk_dmd;
	cos_d[1].offset = len1;

	// dst: copy vcs from src
	for (i = 1; i <= len1; i++)
		vcs_d[-i] = vcs_s[-(o1 + i)];
		
	// scr: remove vcs
	for (i = 1; i <= len2; i++)
		vcs_s[-(o1 + i)] = vcs_s[-(o2 + i)];

	// src: remove co
	for (i = co_index; i < *n_s; i++) {
		cos_s[i].chunk = cos_s[i + 1].chunk;
		cos_s[i].offset = cos_s[i + 1].offset - len1;
	}

	// src: decrease n
	(*n_s)--;
}


/*
 * if @chunk_cow == 0, just set co but vc
 */
void dms_elist_new_co_vc(
	dms_bh_t *elist_bh, 
	uint16 ver, 
	chunk_t chunk_dmd, 
	chunk_t chunk_cow)
{
	int i, o1, o2, o3;
	uint16 *n;
	void *p;
	dms_chunk_offset_t *cos;  //Chunk-Offset
	dms_ver_chunk_t *vcs;  //Ver-Chunk

	p = page_address(elist_bh->b_page);
	n = p; // Number of COs
	cos = p - 2;  // fake cos[0].offset
	vcs = p + 4096;

	/*
	 * the change of elist while o1==2:
	 *
	 *        o1     o2    o3
	 *        |      |     |
	 *     1  2      5  4  3    2  1
	 * n 0 CO CO    [VC VC VC] [VC VC]
	 *               O2         O1
	 *                            
	 *
	 *
	 *     1  2     3     6  5  4    3       2  1
	 * n 0 CO COnew CO   [VC VC VC] [VCnew] [VC VC]
	 */

	// find where to insert new co
	for (o1 = 1; o1 <= *n; o1++)
		if (chunk_dmd < cos[o1].chunk) break;

	o2 = cos[*n].offset;
	o3 = cos[o1 - 1].offset + 1;

	// move the cos 1 right, and increase the offset
	for (i = *n; i >= o1; i--) {
		cos[i + 1].chunk = cos[i].chunk;
		cos[i + 1].offset = cos[i].offset + 1;
	}

	// set the new co
	cos[o1].chunk = chunk_dmd;
	cos[o1].offset = cos[o1 - 1].offset + 1;

	// move the vcs 1 left
	for (i = o2; i >= o3; i--)
		vcs[-i -1] = vcs[-i];

	// set the new vc
	vcs[-o3].ver = ver;
	vcs[-o3].chunk = chunk_cow;

	// increase n
	(*n)++;
}


/*
 * calc how many vc insertable into the elist
 * return n: n more vc space available
 */
int dms_elist_enough_space(dms_bh_t *elist_bh)
{
	int used;
	uint16 n_vc;  // number of version-chunk
	void *p;
	uint16 *n_co;  // number of chunk-offset
	dms_chunk_offset_t *cos;

	p = page_address(elist_bh->b_page);
	n_co = p;
	cos = p - 2;
	n_vc = cos[*n_co].offset;
	used = 4 + (*n_co) * sizeof(dms_chunk_offset_t) + n_vc * sizeof(dms_ver_chunk_t);
	return (4096 - used) / sizeof(dms_ver_chunk_t);
}


/****************
 *
 * exception
 *
 ****************/

/*
 * get @orig->@ver->@chunk_dmd->exception (chunk 0 is reserved, standing for error)
 *
 * return a positive chunk_nr if ok
 * return 0 if there is no exception
 * return ERETRY if reading
 */
chunk_t dms_exception_get(dms_orig_t *orig, uint16 ver, uint32 chunk_dmd)
{
	int co_index, vc_index;
	plot_t index1, index2;
	dms_bh_t *elist_bh;
	chunk_t chunk_cow;

	index1 = dms_index1_get(orig, chunk_dmd);
	if (!index1) goto out;

	index2 = dms_index2_get(orig, chunk_dmd, index1);
	RET_RETRY(index2);
	if (!index2) goto out;

	elist_bh = dms_bh_plot_aread(orig, index2);
	RET_RETRY(elist_bh);

	co_index = dms_elist_find_co(elist_bh, chunk_dmd);
	BUG_ON(!co_index);

	vc_index = dms_elist_find_vc(elist_bh, co_index, ver);

	if (vc_index < 0) goto out;

	chunk_cow = dms_elist_get_vc(elist_bh, vc_index);

	return chunk_cow;

out:
	return 0;
}


/*
 * set '@orig'->'@ver'->'@chunk_dmd'->exception to '@chunk_cow'
 * alloc new index2 plot and elist plot when necessary
 *
 * @chunk_cow == 0 is allowed for reading ahead
 *
 *        index2_plot elist_plot co   vc
 * case1:       o          o     o    o
 * case2:       o          o     o    x
 * case3:       o          x     x    x
 * case4:       x          x     x    x
 *
 *              (o: have   x: not have)
 *
 *
 * (start from case1, end with OK):
 *
 *     (case1)
 *          |
 *  get_index1  -no->  (case4) new_index2_elist_plots
 *          |                                     |
 *  get_index2  -no->  (case3) new_elist_plot     |
 *          |                   |                 |
 *     find_co                 new_co_vc  <-------+
 *          |                   |
 *          |                  [OK]
 *          |
 *     find_vc  -no->  (case2) enough_space  -no->  new_elist_plot
 *          |                   |                    |
 *        [BUG]                new_vc               move_elist_new_vc
 *                              |                    |
 *                             [OK]                 [OK]
 *
 *
 * return 0 if ok
 * return ERETRY if reading
 * return EXX if error
 */
int dms_exception_set(dms_orig_t *orig, uint16 ver, chunk_t chunk_dmd, chunk_t chunk_cow)
{
	uint32 r;
	int bmp_old, bmp;
	dms_cow_t *cow;
	int co_index, vc_index;
	plot_t index1, index2, new_index2;
	dms_bh_t *elist_bh, *new_elist_bh;

	cow = orig->cow;

//case1:
	index1 = dms_index1_get(orig, chunk_dmd);
	if (!index1) goto case4; //goto new index2 plot

	index2 = dms_index2_get(orig, chunk_dmd, index1);
	RET_RETRY(index2);
	if (!index2) goto case3; // goto new elist plot

	elist_bh = dms_bh_plot_aread(orig, index2);
	RET_RETRY(elist_bh);

	co_index = dms_elist_find_co(elist_bh, chunk_dmd);
	BUG_ON(!co_index);
	
	vc_index = dms_elist_find_vc(elist_bh, co_index, ver);
	if (vc_index < 0) {
		vc_index = -vc_index;
		goto case2;
	}

	dms_elist_set_vc(elist_bh, vc_index, chunk_cow);

	dms_bh_awrite(cow, elist_bh);

	goto ok;

case4: // new index2 and elist plots
	r = dms_plot_index2_elist_alloc(orig, &index1,&index2, &elist_bh, chunk_dmd);
	RET_RETRY_EXX(r);

	dms_index1_set(orig, chunk_dmd, index1);
	dms_index2_set(orig, chunk_dmd, index1, index2);

	goto new_co_vc;

case3: // new elist plot
	index2 = dms_plot_elist_alloc(orig, chunk_dmd, index1, PLOT_BMP_USED, &elist_bh);
	RET_RETRY_EXX(index2);

new_co_vc:
	dms_elist_new_co_vc(elist_bh, ver, chunk_dmd, chunk_cow);
	dms_bh_awrite(cow, elist_bh);

	bmp = dms_elist_calc_bmp(elist_bh);
	if (bmp == PLOT_BMP_FULL)
		dms_plot_bitmap_set(orig->cow, index2, bmp);

	goto ok;

case2: // enough space?
	
	bmp_old = dms_elist_calc_bmp(elist_bh);
	
	r = dms_elist_enough_space(elist_bh);

	if (r) {
		dms_elist_new_vc(elist_bh, co_index, vc_index, ver, chunk_cow);
	} else {
		new_index2 = dms_plot_elist_alloc(orig, chunk_dmd, index1, PLOT_BMP_EMPTY, &new_elist_bh);
		RET_RETRY_EXX(new_index2);

		dms_elist_move_co_vc(elist_bh, new_elist_bh, co_index, chunk_dmd);

		vc_index = dms_elist_find_vc(new_elist_bh, 1, ver);
		BUG_ON(vc_index > 0);
		vc_index = -vc_index;
		dms_elist_new_vc(new_elist_bh, 1, vc_index, ver, chunk_cow);

		dms_bh_awrite(cow, new_elist_bh);

		bmp = dms_elist_calc_bmp(new_elist_bh);
		if (bmp == PLOT_BMP_FULL)
			dms_plot_bitmap_set(cow, new_index2, bmp);
	}

	dms_bh_awrite(cow, elist_bh);

	bmp = dms_elist_calc_bmp(elist_bh);
	if (bmp != bmp_old)
		dms_plot_bitmap_set(cow, index2, bmp);

ok:
	if (chunk_cow) {
		VTREE_DISK(ver).nr_exceptions++;
		dms_bh_awrite(cow, VTREE_BH(ver));
	}

	return 0;
}


/*
 * delete the exception, free the corresponding chunk
 * zero index2 if the elist-plot has no exceptions of @chunk_dmd any more
 * zero index1 and free index2-plot if index2-plot is empty
 * 
 * return the deleted chunk number if ok
 * return 0 if no exception
 * return ERETRY if reading
 */ 
int dms_exception_delete(dms_orig_t *orig, uint16 ver_nr, chunk_t chunk_dmd)
{
	int r;
	dms_cow_t *cow;
	dms_bh_t *elist_bh;
	chunk_t chunk_cow;
	int co_index;
	plot_t index1, index2;
	int bmp_old, bmp_new;

	cow = orig->cow;

	index1 = dms_index1_get(orig, chunk_dmd);
	if (!index1) goto none;

	index2 = dms_index2_get(orig, chunk_dmd, index1);
	RET_RETRY(index2);
	if (!index2) goto none;

	elist_bh = dms_bh_plot_aread(orig, index2);
	RET_RETRY(elist_bh);

	co_index = dms_elist_find_co(elist_bh, chunk_dmd);
	if (!co_index) goto none;

	bmp_old = dms_elist_calc_bmp(elist_bh);
	
	chunk_cow = dms_elist_delete_vc(elist_bh, ver_nr, &co_index, 1);
	if (!chunk_cow) goto none;  // elist not changed
	
	bmp_new = dms_elist_calc_bmp(elist_bh);

	if (bmp_old != bmp_new)
		dms_plot_bitmap_set(cow, index2, bmp_new);

	// no writeback if new bitmap is 0
	if (bmp_new)
		dms_bh_awrite(cow, elist_bh);

	// co was deleted
	if (!co_index) {
		r = dms_index2_set(orig, chunk_dmd, index1, 0);

		// index2 plot is empty, free it
		if (!r) {
			dms_index1_set(orig, chunk_dmd, 0);			
			dms_plot_bitmap_set(cow, index1, 0);
		}
	}

	dms_chunk_free(orig->cow, chunk_cow);

	VTREE_DISK(ver_nr).nr_exceptions--;
	dms_bh_awrite(cow, VTREE_BH(ver_nr));

	return chunk_cow;
none:
	return 0;
}


/*
 * return 0 if ok, or no exception exist
 * return ERETRY if reading
 */
int dms_exception_delete_chunk(dms_orig_t *orig, uint16 ver_nr, chunk_t chunk_dmd)
{
	chunk_t chunk_cow;
	
	chunk_cow = dms_exception_delete(orig, ver_nr, chunk_dmd);
	RET_RETRY(chunk_cow);

	if (chunk_cow)
		dms_chunk_free(orig->cow, chunk_cow);

	return 0;	
}


/*
 * move exception from @ver_nr_s to @ver_nr_d
 * 
 * return the moved chunk number if ok
 * return 0 if no exception
 * return ERETRY if reading
 */ 
int dms_exception_move(dms_orig_t *orig, chunk_t chunk_dmd, uint16 ver_nr_d, uint16 ver_nr_s)
{
	dms_cow_t *cow;
	dms_bh_t *elist_bh;
	chunk_t chunk_cow;
	int co_index, vc_index;
	plot_t index1, index2;

	cow = orig->cow;

	index1 = dms_index1_get(orig, chunk_dmd);
	if (!index1) goto none;

	index2 = dms_index2_get(orig, chunk_dmd, index1);
	RET_RETRY(index2);
	if (!index2) goto none;

	elist_bh = dms_bh_plot_aread(orig, index2);
	RET_RETRY(elist_bh);

	co_index = dms_elist_find_co(elist_bh, chunk_dmd);
	BUG_ON(!co_index);

	chunk_cow = dms_elist_delete_vc(elist_bh, ver_nr_s, &co_index, 0);
	BUG_ON(!chunk_cow);

	vc_index = dms_elist_find_vc(elist_bh, co_index, ver_nr_d);
	if (vc_index > 0) {
		BUG_ON(dms_elist_get_vc(elist_bh, vc_index));
		// pre-set-excep might set exception to 0
		dms_elist_set_vc(elist_bh, vc_index, chunk_cow);
	} else {
		vc_index = -vc_index;
		dms_elist_new_vc(elist_bh, co_index, vc_index, ver_nr_d, chunk_cow);
	}
	
	dms_bh_awrite(cow, elist_bh);

	VTREE_DISK(ver_nr_s).nr_exceptions--;
	VTREE_DISK(ver_nr_d).nr_exceptions++;

	dms_bh_awrite(cow, VTREE_BH(ver_nr_d));
	if (VTREE_BH(ver_nr_d) != VTREE_BH(ver_nr_s))
		dms_bh_awrite(cow, VTREE_BH(ver_nr_s));

	return chunk_cow;
none:
	return 0;
}


/*
 * check whether @Father_ver is inherited
 *
 * 1. @F has no children -> 0
 * 2. Foreach C in Children(@F)
 *       has_exception(C) ->  Continue
 *       non_ghost(C)  ->  1
 *       be_inherited(C) ->  1
 * 3. -> 0
 * 
 * return 1 if @Father_ver is inherited
 * return 0 if not
 * return ERETRY if reading
 */
int dms_exception_be_inherited(dms_orig_t *orig, uint16 father_ver, uint32 chunk_dmd)
{
//	uint32 r;
	uint16 child_ver;
	chunk_t exception;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	// no children
	if (!dms_vtree_has_child(orig, father_ver)) {
		return 0;
	}

	// read exception AHEAD
	//for (child_ver = aux[father_ver].child; child_ver; child_ver = aux[child_ver].brother)
		//exception = dms_exception_get(orig, child_ver, chunk_dmd);
	
	// iterate all children
	for (child_ver = aux[father_ver].child; child_ver; child_ver = aux[child_ver].brother) {
		exception = dms_exception_get(orig, child_ver, chunk_dmd);
		RET_RETRY(exception);

		// cut off the recursive ghost check. as kernel stack is only 8K
		if (!exception) goto ok;

		/*
		if (exception) continue;

		if (!dms_vtree_is_ghost(orig, child_ver)) goto ok;

		r = dms_exception_be_inherited(orig, child_ver, chunk_dmd);
		RET_RETRY(r);

		if (r) goto ok;
		*/
	}

	return 0;
ok:
	return 1;
}


/*
 * the refered version will be saved in @refered_ver_nr if it's NOT NULL
 *
 * return the chunk_nr if @ver_nr has exception chunk
 * return 0 if it refers to orig dev
 * return ERETRY if reading
 */
chunk_t dms_exception_reference(
	dms_orig_t *orig, 
	uint16 ver, 
	uint32 chunk_dmd, 
	uint16 *ver_refer)
{
	uint32 chunk_cow;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	do {
		chunk_cow = dms_exception_get(orig, ver, chunk_dmd);
		RET_RETRY(chunk_cow);

		if (chunk_cow) {
			if (ver_refer) *ver_refer = ver;
			return chunk_cow;
		}

		ver = dms_vtree_father(orig, ver);
	} while (ver);

	if (ver_refer) *ver_refer = 0;

	return 0;
}


/****************
 *
 * rollback
 *
 ****************/


void dms_rollback_info_init(dms_rollback_info_t *ri)
{	
	memset(ri, 0, sizeof(dms_rollback_info_t));
	dms_job_init(&ri->job, DMS_JOB_TYPE_ROLLBACK);
}


void dms_rollback_job_done(dms_orig_t *orig, dms_rollback_info_t *ri)
{
	int i;

	dms_job_done(g_job);

	// free unused chunks
	for (i = 0; i < 1024; i++) {
		if (ri->chunk_alloc[i]) {
			dms_chunk_free(orig->cow, ri->chunk_alloc[i]);
			 ri->chunk_alloc[i] = 0;
		}
	}

	if (!dms_job_io_done(&ri->job))
		return;

	for (i = 0; i < 1024; i++) {
		dms_chunk_copy_free_bio(&ri->chunk_copy1[i]);
		dms_chunk_copy_free_bio(&ri->chunk_copy2[i]);
	}

	
	if (!ri->index2_plot_nr || g_job->error) {
		// wake up user cmd 'dmsetup'
		if (ri->process_task)
			wake_up_process(ri->process_task);

		if (g_job->error)
			DMERR("rollback (%u,%s,%s) error", 
				ri->tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
		else
			DMINFO("rollback (%u,%s,%s) done", 
				ri->tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);

		ri->tag = 0;

		dms_job_unplug(orig->cow, g_job);

		return;
	}

	
	// re-init ri to rollback the next index2_plot
	ri->index1_idx++;
	ri->index2_plot_nr = 0;
	ri->index2_plot_bh = 0;
	memset(ri->plot_rollback_done, 0, sizeof(ri->plot_rollback_done));
	dms_job_wake_up(orig->cow);
}


/*
 * read the index2 plot
 *
 * return 0 if ok
 * return ERETRY if reading
 */
int dms_rollback_read_index2_plot(dms_orig_t *orig, dms_rollback_info_t *ri)
{
	dms_bh_t *bh;

	bh = dms_bh_plot_aread(orig, ri->index2_plot_nr);
	RET_RETRY(bh);

	ri->index2_plot_bh = bh;
	ri->p_index2_plot = page_address(bh->b_page);

	return 0;
}


/*
 * alloc chunk v2, copy v0->v2, v1->v0
 *
 * v0
 *  +-v2
 *     +-v1*
 *
 * source holding retry:
 *    ri->chunk_alloc
 *
 * return 0 if ok
 * return ERETRY if reading
 * return EXX if error
 */
int dms_rollback_chunk_copy_2(
	dms_orig_t *orig, dms_rollback_info_t *ri,
	chunk_t chunk_dmd, chunk_t chunk_cow, 
	uint16 son_ver_nr)
{
	uint32 r, r1, r2;
	int idx;
	dms_cow_t *cow;
	chunk_t *chunk_son;
	dms_chunk_copy_t *chunk_copy1, *chunk_copy2;

	cow = orig->cow;
	idx = RIGHTBITS32(chunk_dmd, 10);
	
	chunk_copy1 = &ri->chunk_copy1[idx];
	chunk_copy2 = &ri->chunk_copy2[idx];
	chunk_son  = &ri->chunk_alloc[idx];

	if (*chunk_son == 0) {
		*chunk_son = dms_chunk_alloc(cow);
		RET_EXX(*chunk_son);
	}

	// pre set exception
	r = dms_exception_set(orig, son_ver_nr, chunk_dmd, 0);
	RET_EXX(r);

	// copy v0 -> v2, v2 -> v0
	r1 = dms_chunk_copy(orig, chunk_copy1, cow->bdev, *chunk_son, orig->bdev, chunk_dmd);
	r2 = dms_chunk_copy(orig, chunk_copy2, orig->bdev, chunk_dmd, cow->bdev, chunk_cow);

	RET_RETRY(r1);
	RET_RETRY(r2);

	r = dms_exception_set(orig, son_ver_nr, chunk_dmd, *chunk_son);
	RET_RETRY_EXX(r);

	*chunk_son = 0;

	return 0;
}


/*
 * copy v1 to v0
 *
 * v0
 *  +-v1(*)
 *
 * return 0 if ok
 * return ERETRY if reading
 */
int dms_rollback_chunk_copy_1(dms_orig_t *orig, chunk_t chunk_dmd, chunk_t chunk_cow)
{
	uint32 r;
	dms_chunk_copy_t *chunk_copy;

	chunk_copy = &orig->rollback_info.chunk_copy1[RIGHTBITS32(chunk_dmd, 10)];

	r = dms_chunk_copy(orig, chunk_copy, orig->bdev, chunk_dmd, orig->cow->bdev, chunk_cow);
	RET_RETRY(r);

	return 0;
}


/*
 * 1. refer to orig
 *      a. out
 * 2. refer to child of orig
 *      a. copy ref to orig
 *      b. delete ref
 * 3. child of orig has exception
 *      a. copy ref to orig
 * 4. child of orig has NO excetopn
 *      a. copy orig to child of orig, copy ref to orig
 *
 * return 0 if ok
 * return ERETRY if reading
 * return EXX if error
 */
int dms_rollback_elist_plot(dms_orig_t *orig, dms_rollback_info_t *ri, chunk_t chunk_dmd)
{
	uint32 r;
	uint16 son_ver_nr, refered_ver_nr;
	chunk_t chunk_cow, chunk_son;

	chunk_cow = dms_exception_reference(orig, ri->ver_nr, chunk_dmd, &refered_ver_nr);
	RET_RETRY(chunk_cow);

	// 1
	if (!chunk_cow) goto out;

	// 2
	if (dms_vtree_father(orig, refered_ver_nr) == 0) {
		r = dms_rollback_chunk_copy_1(orig, chunk_dmd, chunk_cow);
		RET_RETRY(r);

		r = dms_exception_delete(orig, refered_ver_nr, chunk_dmd);
		RET_RETRY(r);

		goto out;
	}

	son_ver_nr = dms_vtree_1st_child(0, orig);
	chunk_son = dms_exception_get(orig, son_ver_nr, chunk_dmd);
	RET_RETRY(chunk_son);
	
	// 3
	if (chunk_son) {
		r = dms_rollback_chunk_copy_1(orig, chunk_dmd, chunk_cow);
		RET_RETRY(r);
		goto out;
	}

	// 4
	r = dms_rollback_chunk_copy_2(orig, ri, chunk_dmd, chunk_cow, son_ver_nr);
	RET_RETRY_EXX(r);
	goto out;

out:
	return 0;
}


/*
 * iterate over the 1024 elist plots pointed by the index2 plot
 *
 * return 0 if ok
 * return ERETRY if any one of the 1024 elist plot is reading
 * return EXX if error
 */
int dms_rollback_elist_plots(dms_orig_t *orig, dms_rollback_info_t *ri)
{
	int i;
	uint32 r;
	uint32 ret;
	chunk_t chunk_dmd;

	ret = 0;

	for (i = 0; i < 1024; i++) {
		if (ri->p_index2_plot[i] && !dms_read_bit(ri->plot_rollback_done, i)) {
			chunk_dmd = (ri->index1_idx << 10) | i;

			r = dms_rollback_elist_plot(orig, ri, chunk_dmd);

			RET_EXX(r);
			REMB_RETRY(ret, r);
			if (!IS_RETRY(r))
				dms_write_bit(ri->plot_rollback_done, i, 1);
			
		}
	}

	return ret;
}


/*
 * find out the next index2 plot (pointed by a non-zero index1) to rollback
 *
 * return index2 plot number if found
 * return 0 if finding reached the end
 */
plot_t dms_rollback_find_index2_plot(dms_orig_t *orig, dms_rollback_info_t *ri)
{
	plot_t index1;
	
	while (ri->index1_idx <= orig->max_index1_idx) {
		index1 = INDEX1_DISK_BY_IDX(ri->index1_idx);
		if (index1) return index1;
		ri->index1_idx++;
	}

	return 0;
}


/*
 * rollback snapshot index2-plot by index2-plot
 */
int dms_rollback_job(dms_rollback_info_t *ri, dms_orig_t *orig)
{
	uint32 r;

	if (unlikely(g_job->error)) goto done;

	// update the ver_nr, as tag-version pair might change
	ri->ver_nr = dms_vtree_get_version_by_tag(orig, ri->tag);
	BUG_ON(!ri->ver_nr);

	if (!ri->index2_plot_nr) {
		ri->index2_plot_nr = dms_rollback_find_index2_plot(orig, ri);
		if (!ri->index2_plot_nr) goto done;
	}

	if (!ri->index2_plot_bh) {
		r = dms_rollback_read_index2_plot(orig, ri);
		RET_RETRY(r);
	}

	r = dms_rollback_elist_plots(orig, ri);
	RET_RETRY(r);

	if (IS_EXX(r)) {
		g_job->error = 1;
		dms_cow_mark_invalid(orig->cow);
	}

done:
	if (dms_job_io_done(g_job))
		dms_rollback_job_done(orig, ri);

	return 0;	
}


/****************
 *
 * clean
 *
 ****************/


/*
 * G+
 * +-L*
 * +-..
 * +-L*
 *
 * merge the exception of Ghost to it's children:
 * 1. if all children have exception, delete the exception of Ghost
 * 2. if only 1 child has no exception, move the exception of Ghost to the child
 *
 * return 0 if ok
 * return ERETRY if reading
 * return EXX if error
 */
int dms_clean_merge_exception(dms_orig_t *orig, uint16 ver_nr, chunk_t chunk_dmd)
{
	uint32 r;
	chunk_t chunk_live, chunk_ghost;
	uint16 ver_nr_live;
	int nr_minors, ver_nr_saved = 0;
	dms_vtree_aux_t *aux;

	aux = orig->vtree.auxiliary;

	// if the Ghost has no exception, exit
	chunk_ghost = dms_exception_get(orig, ver_nr, chunk_dmd);
	RET_RETRY(chunk_ghost);
	if (!chunk_ghost) return 0;

	// the number of verisons has no exceptions
	nr_minors = 0;
	ver_nr_live = dms_vtree_1st_child(ver_nr, orig);

	// count the number of children without exceptions
	while (ver_nr_live) {
		chunk_live = dms_exception_get(orig, ver_nr_live, chunk_dmd);
		RET_RETRY(chunk_live);		
		
		if (!chunk_live) {
			nr_minors++;
			if (nr_minors > 1) goto out;
			ver_nr_saved = ver_nr_live;
		}
		ver_nr_live = aux[ver_nr_live].brother; 
	}

	// all children has exceptions. delete the exceptions of Ghost
	if (!nr_minors) {
		r = dms_exception_delete(orig, ver_nr, chunk_dmd);
		RET_RETRY(r);
	}

	// only 1 ver_nr_live has no exceptions. move the exceptions of Ghost to the ver_nr_live
	if (nr_minors == 1) {
		r = dms_exception_move(orig, chunk_dmd, ver_nr_saved, ver_nr);
		RET_RETRY(r);
	}

out:
	return 0;
}



/*
 * return 0 if ok
 * return ERETRY if readin
 * return EXX if error
 */
int dms_clean_elist_plot(dms_orig_t *orig, dms_clean_info_t *ci, chunk_t chunk_dmd)
{
	uint32 r, ret;
	uint16 ver_nr;

	ret = 0;

	for (ver_nr = 1; ver_nr < MAX_NR_OF_VERSIONS; ver_nr++) {
		if (dms_vtree_trigger_delete(orig, ver_nr)) {
			r = dms_exception_delete(orig, ver_nr, chunk_dmd);
			REMB_RETRY(ret, r);
		}

		if (dms_vtree_trigger_merge(orig, ver_nr) || 
			dms_vtree_trigger_merge_2(orig, ver_nr))
		{
			r = dms_clean_merge_exception(orig, ver_nr, chunk_dmd);
			RET_EXX(r);
			REMB_RETRY(ret, r);
		}
	}

	return ret;
}


/*
 * return 0 if ok
 * return ERETRY if readin
 * return EXX if error
 */
int dms_clean_elist_plots(dms_orig_t *orig, dms_clean_info_t *ci)
{
	uint32 i, r, ret;
	chunk_t chunk_dmd;

	ret = 0;

	for (i = 0; i < 1024; i++) {
		// no elist plot
		if (!ci->p_index2_plot[i])
			continue;

		chunk_dmd = (ci->index1_idx << 10) | i;
		
		r = dms_clean_elist_plot(orig, ci, chunk_dmd);

		RET_EXX(r);
		REMB_RETRY(ret, r);
	}

	return ret;
}


/*
 * read the index2 plot
 *
 * return 0 if ok
 * return ERETRY if reading
 */
int dms_clean_read_index2_plot(dms_orig_t *orig, dms_clean_info_t *ci)
{
	dms_bh_t *bh;

	bh = dms_bh_plot_aread(orig, ci->index2_plot_nr);
	RET_RETRY(bh);

	ci->index2_plot_bh = bh;
	ci->p_index2_plot = page_address(bh->b_page);

	return 0;
}


/*
 * find out the next index2 plot (pointed by a non-zero index1) to clean
 *
 * return the index2 plot number.  (It's a fatal error if not found)
 */
plot_t dms_clean_find_index2_plot(dms_orig_t *orig, dms_clean_info_t *ci)
{
	plot_t index1;
	
retry:
	while (ci->index1_idx <= orig->max_index1_idx) {
		index1 = INDEX1_DISK_BY_IDX(ci->index1_idx);
		if (index1) return index1;
		ci->index1_idx++;
	}

	// not found. find from the start
	ci->index1_idx = 0;
	goto retry;
}


void dms_clean_job_done(dms_orig_t *orig, dms_clean_info_t *ci)
{
	// reset the clean_info struct
	ci->index2_plot_nr = 0;
	ci->index2_plot_bh = 0;
	
	dms_job_done(g_job);

	if (g_job->error) {
		dms_io_err(orig->cow->bdev, "clean");
		dms_job_unplug(orig->cow, g_job);
		return;
	}

	// stop cleaning if orig is freeing
	if (!orig->freeing && dms_vtree_clean(orig)) {
		ci->index1_idx++;
		dms_job_wake_up(orig->cow);

		return;
	}

	// dms_vtree_clean() might raise IO operation
	if (dms_job_io_done(g_job)) {
		dms_job_unplug(orig->cow, g_job);
	}
}


void dms_clean_info_init(dms_clean_info_t *ci)
{
	dms_job_init(&ci->job, DMS_JOB_TYPE_CLEAN);
}


/*
 * clean job routine
 * find out the ghost versions which can be deleted or merged,
 * clean them index2_plot by index2_plot
 *
 */
int dms_clean_job(dms_clean_info_t *ci, dms_orig_t *orig)
{
	uint32 r;

	if (unlikely(g_job->error)) goto done;
	
	r = dms_vtree_clean(orig);
	if (!r) goto done;

	if (!ci->index2_plot_nr) {
		// stop cleaning before next index2-plot-cleaning starting, if orig is freeing, 
		if (orig->freeing) goto done;
		ci->index2_plot_nr = dms_clean_find_index2_plot(orig, ci);
	}

	if (!ci->index2_plot_bh) {
		r = dms_clean_read_index2_plot(orig, ci);
		RET_RETRY(r);
	}

	r = dms_clean_elist_plots(orig, ci);
	RET_RETRY(r);

	if (IS_EXX(r)) {
		g_job->error = 1;
		dms_cow_mark_invalid(orig->cow);
	}

done:
	if (dms_job_io_done(g_job))
		dms_clean_job_done(orig, ci);

	return 0;
}


void dms_map_check_overlap_bios(dms_map_info_t *mi, dms_bio_t *bio)
{
	int not_overlap;
	dms_list_t *head;
	dms_bio_t *bio1;

	head = mi->dmd_bios;
	while (head) {
		bio1 = (dms_bio_t*)head->load;
		not_overlap = (bio->bi_sector + bio_sectors(bio) - 1 <= bio1->bi_sector) ||
		(bio1->bi_sector + bio_sectors(bio1) - 1 <= bio->bi_sector);
		BUG_ON(!not_overlap);

		head = head->next;
	}
}

/*
 * group bios of same chunk number
 *
 * return mi if grouped
 * return 0 if not
 */
dms_map_info_t* dms_map_group_bio(dms_dm_dev_t *dmd, dms_bio_t *bio, chunk_t chunk_dmd, int map_type)
{
 	int idx;
	dms_map_info_t *mi;
	struct list_head *hash_head;

	idx = map_type - 1;	
	hash_head = &dmd->bio_group_hash[idx][BIO_GROUP_HASH_FUN(chunk_dmd)];

	list_for_each_entry(mi, hash_head, group_bio_node) {
		if (mi->chunk_dmd == chunk_dmd) {
			dms_map_check_overlap_bios(mi, bio);
	
			dms_list_add_load(&mi->dmd_bios, bio);
			mi->bios_size += bio_sectors(bio);

			return mi;
		}
	}

	return 0;
}


/*
 *dequeue mi from bio group hash
 */
void dms_map_group_dehash_mi(dms_map_info_t *mi)
{
	list_del(&mi->group_bio_node);
}


void dms_map_group_enhash_mi(dms_map_info_t *mi)
{
	int idx = mi->map_type - 1;
	dms_dm_dev_t *dmd = mi->dmd;
	struct list_head *list_head = &dmd->bio_group_hash[idx][BIO_GROUP_HASH_FUN(mi->chunk_dmd)];
		
	list_add(&mi->group_bio_node, list_head);
}


/****************
 *
 * bio mapping
 *
 ****************/


void dms_map_bio_to_orig(struct bio *bio, dms_orig_t *orig)
{
	bio->bi_bdev = orig->bdev;
}


/*
 * map bio to cow device
 */
void dms_map_bio_to_cow(dms_map_info_t *mi, struct bio *bio)
{
	dms_cow_t *cow = mi->dmd->orig->cow;
	int sector_shift = cow->sector_shift;

	bio->bi_bdev = cow->bdev;
	bio->bi_sector = ((uint64)(mi->chunk_cow) << sector_shift) |
		RIGHTBITS64(bio->bi_sector, sector_shift);
}


void dms_map_submit_bios(dms_map_info_t *mi)
{
	dms_bio_t *bio;
	dms_job_t *job = &mi->job;

	while ((bio = dms_list_fetch_load(&mi->dmd_bios))) {
		// IOs on orig can go on even if there is error
		if (job->error && (mi->map_type != DMS_MAP_TYPE_WO)) {
			if (bio->bi_end_io) {
				if (mi->map_type == DMS_MAP_TYPE_WS)
					atomic_inc(&mi->ws_dmd_bio_count);
				bio->bi_end_io(bio, 1);
			}
			continue;
		}

		if (mi->map_type == DMS_MAP_TYPE_WS)
			atomic_inc(&mi->ws_dmd_bio_count);
		
		if (mi->chunk_cow)
			dms_map_bio_to_cow(mi, bio);
		else
			dms_map_bio_to_orig(bio, mi->dmd->orig);

		generic_make_request(bio);
	}
}


void dms_map_info_init(
	dms_map_info_t *mi, 
	dms_dm_dev_t *dmd, 
	struct bio *bio, 
	chunk_t chunk_dmd, 
	int map_type)
{
	memset(mi, 0, sizeof(*mi));

	dms_job_init(&mi->job, DMS_JOB_TYPE_MAP);
	
	mi->dmd = dmd;
	mi->bios_size = bio_sectors(bio);
	mi->map_type = map_type;
	mi->chunk_dmd = chunk_dmd;

	dms_list_add_load(&mi->dmd_bios, bio);
	spin_lock_init(&mi->ws_dms_bio_lock);
	BUG_ON(atomic_read(&mi->ws_dmd_bio_count));

	INIT_LIST_HEAD(&mi->group_bio_node);
}


/*
 * alloc a chunk, copy chunk if necessary, set an exception point to the new chunk
 *
 * source holding retry:
 *   mi->chunk_alloc
 *
 * return the new alloced chunk nr if ok
 * return ERETRE if reading
 * return EXX if error
 */
int dms_map_cow(dms_map_info_t *mi, dms_orig_t *orig, int ver)
{
	uint32 r;
	chunk_t chunk_dmd, chunk_alloc, chunk_refer;
	dms_cow_t *cow;

	BUG_ON(!ver);

	cow = orig->cow;
	chunk_dmd = mi->chunk_dmd;
	chunk_alloc = mi->chunk_alloc;

	// alloc a chunk
	if (!chunk_alloc) {
		chunk_alloc = dms_chunk_alloc(orig->cow);
		RET_EXX(chunk_alloc);
		mi->chunk_alloc = chunk_alloc;
	}

	// copy orig chunk if write orig
	if (mi->map_type == DMS_MAP_TYPE_WO) {
		r = dms_chunk_copy(
				orig, &mi->chunk_copy, 
				orig->cow->bdev, chunk_alloc, 
				orig->bdev, chunk_dmd
			);
		RET_RETRY(r);
	}

	// copy orig chunk if write snap chunk-partly
	if (mi->map_type == DMS_MAP_TYPE_WS) {

		BUG_ON(mi->bios_size > (cow->misc.disk->chunk_size << 3));

		if (mi->bios_size < (cow->misc.disk->chunk_size << 3)) {
			uint16 ver_refer; 

			chunk_refer = dms_exception_reference(orig, ver, chunk_dmd, &ver_refer);
			RET_RETRY(chunk_refer);

			if (chunk_refer) {
				// copy from cow to cow
				r = dms_chunk_copy(
						orig, &mi->chunk_copy, 
						cow->bdev, chunk_alloc, 
						cow->bdev, chunk_refer
					);
			} else {
				// copy from orig to cow
				r = dms_chunk_copy(
						orig, &mi->chunk_copy, 
						cow->bdev, chunk_alloc, 
						orig->bdev, chunk_dmd
					);
			}
			RET_RETRY(r);
		}
	}

	// set the newly allocated cow chunk as the exception of the orig chunk of @ver
	r = dms_exception_set(orig, ver, chunk_dmd, chunk_alloc);
	RET_RETRY_EXX(r);


	mi->chunk_alloc = 0;

	return chunk_alloc;
}


/*
 * reutrn 0 if bio mapped
 * reutrn ERETRY if reading
 * return EXX if error
 */
static int dms_map_write_orig(dms_map_info_t *mi)
{
	uint32 r;
	int child_ver;
	int nr_child;
	chunk_t exception;

	dms_orig_t *orig = mi->dmd->orig;
	nr_child = dms_vtree_has_child(orig, 0);
	child_ver = dms_vtree_1st_child(0, orig);

	BUG_ON(nr_child > 1);

	if (nr_child == 0) goto ok;

	exception = dms_exception_get(orig, child_ver, mi->chunk_dmd);
	RET_RETRY(exception);

	if (!exception) {
		r = dms_map_cow(mi, orig, child_ver);
		RET_RETRY_EXX(r);
	}

ok:
	return 0;
}

/*
 * reutrn 0 if bio mapped
 * reutrn ERETRY if reading
 * return EXX if error
 */
int dms_map_write_snap(dms_map_info_t *mi)
{
	uint32 r;
	uint16 ver;
	chunk_t chunk_cow;
	dms_orig_t *orig;

	orig = mi->dmd->orig;
	ver = atomic_read(&mi->dmd->ver);

	r = dms_exception_be_inherited(orig, ver, mi->chunk_dmd);
	RET_RETRY(r);

	// if the chunk is inherited, freeze it's exception, relocate current tag to a new version
	if (r) {
		ver = dms_vtree_relocate_tag(orig, ver);
		RET_EXX(ver);
		atomic_set(&mi->dmd->ver, ver);
	}

	chunk_cow = dms_exception_get(orig, ver, mi->chunk_dmd);
	RET_RETRY(chunk_cow);

	if (!chunk_cow) {
		chunk_cow = dms_map_cow(mi, orig, ver);
		RET_RETRY_EXX(chunk_cow);
	}

	mi->chunk_cow = chunk_cow;

	return 0;
}


static void dms_map_read_orig(struct bio *bio, dms_dm_dev_t *dmd)
{
	dms_map_bio_to_orig(bio, dmd->orig);

	generic_make_request(bio);
}


/*
 * reutrn 0 if bio mapped
 * reutrn ERETRY if reading
 */
static int dms_map_read_snap(dms_map_info_t *mi)
{
	uint16 ver;
	chunk_t chunk_cow;

	ver = atomic_read(&mi->dmd->ver);
	
	chunk_cow = dms_exception_reference(mi->dmd->orig, ver, mi->chunk_dmd, 0);
	RET_RETRY(chunk_cow);

	mi->chunk_cow = chunk_cow;

	return 0;
}


void dms_map_ws_wake(dms_map_info_t *mi)
{
	unsigned long flags;
	dms_orig_t *orig = dms_job_2_orig(&mi->job);
	
	spin_lock_irqsave(&mi->ws_dms_bio_lock, flags);
	atomic_dec(&mi->ws_dmd_bio_count);
	dms_job_do_wake_up(orig->cow);
	spin_unlock_irqrestore(&mi->ws_dms_bio_lock, flags);
}


/*
 * get the nr of bios which is submitted but end_io not called yet
 */
int dms_map_ws_dmd_bio(dms_map_info_t *mi)
{
	int r;
	unsigned long flags;

	spin_lock_irqsave(&mi->ws_dms_bio_lock, flags);
	r = atomic_read(&mi->ws_dmd_bio_count);
	spin_unlock_irqrestore(&mi->ws_dms_bio_lock, flags);

	return r;
}


void dms_map_job_done(dms_orig_t *orig, dms_map_info_t *mi)
{
	dms_job_t *job = g_job;
	
	if (mi->chunk_alloc) {
		dms_chunk_free(orig->cow, mi->chunk_alloc);
		mi->chunk_alloc = 0;

		if (!dms_job_io_done(job))
			return;
	}

	dms_map_submit_bios(mi);
	if (dms_map_ws_dmd_bio(mi))
		return;

	dms_chunk_copy_free_bio(&mi->chunk_copy);

	dms_map_group_dehash_mi(mi);

	dms_job_done(job);

	dms_job_unplug(orig->cow, job);

	kfree(mi);
}


/*
 * return 0 if job done
 * return ERETRY if reading
 */
int dms_map_job(dms_map_info_t *mi, dms_orig_t *orig)
{
	uint32 r;

	if (unlikely(mi->done)) goto done;
	if (unlikely(g_job->error)) goto done;

	switch (mi->map_type) {
	case DMS_MAP_TYPE_WO:
		r = dms_map_write_orig(mi);
		break;
	case DMS_MAP_TYPE_RS:
		r = dms_map_read_snap(mi);
		break;
	case DMS_MAP_TYPE_WS:
		r = dms_map_write_snap(mi);
		break;
	default:
		BUG_ON(1);
	}

	RET_RETRY(r);

	if (IS_EXX(r)) {
		g_job->error = 1;
		dms_cow_mark_invalid(orig->cow);
	}

	mi->done = 1;

done:
	if (dms_job_io_done(g_job))
		dms_map_job_done(orig, mi);

	return 0;
}


/****************
 *
 * cmd msg handlers
 *
 ****************/


/*
 * create a snapshot of origin.
 *
 * return the tag of new snapshot if ok
 * return EXX if error
 */
int dms_cmd_snap_orig(dms_dm_dev_t *dmd, dms_parsed_args_t *pargs)
{
	return dms_vtree_snap_orig(dmd->orig, pargs->memo);
}


/*
 * take a snapshot of a snapshot
 *
 * ss [tag] [commnet]
 *
 * return tag of new snapshot if ok
 * return EXX if error
 */
int dms_cmd_snap_snap(dms_dm_dev_t *dmd, dms_parsed_args_t *pargs)
{
	int r;
	dms_orig_t *orig;
	uint16 child_tag;
	uint16 child_ver, father_ver;
	dms_vtree_aux_t *aux;

	orig = dmd->orig;
	aux = orig->vtree.auxiliary;

	LOCK_ORIG();

	if (pargs->tag) {
		father_ver = dms_vtree_get_version_by_tag(orig, pargs->tag);
		if (!father_ver) {
			DMERR("(%u,%s,%s) not found", 
				pargs->tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
			r = -EINVAL;
			goto out;
		}
	} else {
		pargs->tag = dmd->tag; // save tag for caller to print msg
		father_ver = atomic_read(&dmd->ver);
	}
	
	child_ver = dms_vtree_find_unused_ver(orig);
	if (!child_ver) {
		r = -ENOSPC;
		goto out;
	}

	child_tag = dms_vtree_find_unused_tag(orig);

	dms_vtree_add_child(orig, father_ver, child_ver, child_tag, pargs->memo);

	r =dms_bh_swrite(VTREE_BH(child_ver));
	if (r) {
		dms_io_err(orig->cow->bdev, "version tree");
		goto out;
	}

	r = child_tag;

out:
	UNLOCK_ORIG();	
	return r;
}


/*
 * delete a origin - unbind orig-dev from cow-dev
 *
 * the user-space caller should remove the orig device after my return
 *
 * do
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_cmd_delete_orig(dms_dm_dev_t *dmd, dms_parsed_args_t *pargs)
{
	uint32 r = 0;
	dms_orig_t *orig;

	orig = dmd->orig;

	LOCK_ORIG();

	if (dms_vtree_has_child(orig, 0)) {
		r = -EBUSY;
		DMERR("can not delete (%s,%s) with children", 
			dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
		);
		goto out;
	}

	// unbind orig from cow
	orig->cow->misc.disk->orig_dev_ids[orig->slot_id][0] = 0;

	r = dms_bh_swrite(orig->cow->misc.bh);
	if (IS_EXX(r)) {
		dms_io_err(orig->cow->bdev, "unbind");
		goto out;
	}
	
out:
	UNLOCK_ORIG();
	return r;
}


/*
 * delete a snapshot
 *
 * the user-space caller should release the snapshot device first
 *
 * ds <tag>
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_cmd_delete_snap(dms_dm_dev_t *dmd, dms_parsed_args_t *pargs)
{
	int r = 0;
	dms_orig_t *orig;
	uint16 ver, tag;

	orig = dmd->orig;
	tag = pargs->tag;

	LOCK_ORIG();
	LOCK_G_LISTS();

	ver = dms_vtree_get_version_by_tag(orig, tag);
	if (!ver) {
		r = -EINVAL;
		DMERR("(%u,%s,%s) not found", 
			tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
		);
		goto out;
	}

	if (dms_dmd_find_by_dd(dmd->dd_orig, dmd->dd_cow, tag)) {
		r = -EBUSY;
		DMERR("can not delete (%u,%s,%s) which is in use", 
			tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
		);
		goto out;
	}

	DMINFO("(%u,%s,%s) has been scheduled to delete",
		tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
	);

	dms_vtree_ghost(orig, ver);

	r = dms_bh_swrite(VTREE_BH(ver));
	if (r) 
		dms_io_err(orig->cow->bdev, "vtree");

out:
	UNLOCK_G_LISTS();
	UNLOCK_ORIG();
	return r;
}


/*
 * rollback a snapshot
 *
 * rs <tag> [b]
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_cmd_rollback_snap(dms_dm_dev_t *dmd, dms_parsed_args_t *pargs)
{
	int r;
	dms_job_t *job;
	dms_orig_t *orig;
	uint16 ver, tag;

	orig = dmd->orig;
	job = &orig->rollback_info.job;
	tag = pargs->tag;

	LOCK_ORIG();

	ver = dms_vtree_get_version_by_tag(orig, tag);
	if (!ver) {
		r = -EINVAL;
		DMERR("(%u,%s,%s) not found", 
			tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
		);
		goto bad;
	}

	if (!list_empty(&orig->rollback_info.job.job_node)) {
		r = -EBUSY;
		DMINFO("(%u,%s,%s) is rollbacking, ignore %u",
			orig->rollback_info.tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev), tag
		);
		goto bad;
	}

	dms_rollback_info_init(&orig->rollback_info);

	orig->rollback_info.tag = tag;

	dms_job_plug(orig->cow, job);

	// make the caller wait rollbacking
	if (!pargs->background) {
		set_current_state(TASK_INTERRUPTIBLE);
		orig->rollback_info.process_task = current;
	}

	dms_job_wake_up(orig->cow);

	UNLOCK_ORIG();

	DMINFO("(%u,%s,%s) has been scheduled to rollback", 
		tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
	);

	// make the caller wait rollbacking
	if (!pargs->background)
		schedule();

	return 0;

bad:
	UNLOCK_ORIG();
	return r;
}


/****************
 *
 * debug msg handlers
 *
 ****************/


void dms_dbg_print_page(uint32 *p)
{
	int i, j;
	int col = 32;
	
	for (i = 0; i < 1024 / col; i++) {
		for (j = 0; j < col; j++)
			if (p[i * col + j])
				break;
			
		if (j == col) 
			continue;

		printk("%u: ", i * col);

		for (j = 0; j < col; j++) {
			printk("%x ", p[i*col+j]);
			if ((j & 0xf) == 0xf)
				printk(" | ");
		}

		printk("\n--\n");
	}
}


void dms_dbg_dump_cow_page(dms_orig_t *orig, int page_idx)
{
	dms_bh_t *bh;

	DMINFO("dump cow page  %u :", page_idx);
	bh = dms_bh_sread(orig->cow->bdev, page_idx);
	if (!bh) {
		DMERR("get buffer bh failed");
		return;
	}

	dms_dbg_print_page(page_address(bh->b_page));
	brelse(bh);
}


void dms_dbg_dump_elist_bh(dms_bh_t *elist_bh)
{
	void *p;
	int i, j;
	uint16 n;
	uint16 *nn;
	dms_chunk_offset_t *cos;
	dms_ver_chunk_t *vcs;

	p = page_address(elist_bh->b_page);
	n = *(uint16*)p;
	nn = p;
	cos = p-2;
	vcs = p + 4096;

/*
	printk(
		"dump_elist_cache: b_blocknr=%llu n=%hu nn=%hu p=%p\n", 
		elist_bh->b_blocknr, n, *nn, p
	);
*/
	
	for (i=1; i<=n; i++) {
		printk("%d#%d ", cos[i].chunk, cos[i].offset);
		if (i%10==0) printk("\n");
	}
	printk("\n");
	
	for (i=n; i>=1; i--) {
		printk("[%d:", cos[i].offset);
		for (j=cos[i].offset; j>cos[i-1].offset; j--)
			printk("%d#%d ", vcs[-j].ver, vcs[-j].chunk);
		printk("]");
		if (i%10==0) printk("\n");
	}
	
	printk("\n");
}


void dms_dbg_dump_elist(dms_orig_t *orig, plot_t elist_plot_nr)
{
	dms_bh_t *bh;

	elist_plot_nr += PLOT_ZONE_P;

	bh = dms_bh_sread(orig->cow->bdev, elist_plot_nr);
	if (!bh) {
		DMERR("get buffer bh failed");
		return;
	}

	dms_dbg_dump_elist_bh(bh);
	
	brelse(bh);
}


void dms_dbg_dump_plot(dms_orig_t *orig, plot_t plot_nr)
{
	DMINFO("dump plot %u :", plot_nr);
	dms_dbg_dump_cow_page(orig, PLOT_ZONE_P + plot_nr);
}


/****************
 *
 * job 2/2
 *
 ****************/


void dms_job_run(dms_job_t *job)
{
	dms_orig_t *orig;

	g_job = job;
	orig = dms_job_2_orig(job);

	LOCK_ORIG();

	switch (job->job_type)
	{
	case DMS_JOB_TYPE_MAP:
		dms_map_job(JOB_2_MI(job), orig);
		break;
	case DMS_JOB_TYPE_CLEAN:
		dms_clean_job(JOB_2_CI(job), orig);
		break;
	case DMS_JOB_TYPE_ROLLBACK:
		dms_rollback_job(JOB_2_RI(job), orig);
		break;
	}

	UNLOCK_ORIG();
}


int dms_job_thread(void *thread_arg)
{
	unsigned long flags;
	dms_cow_t *cow = thread_arg;
	dms_job_thread_t *jt = &cow->job_thread;
	dms_job_t *job, *safe;
	int sec;

	DMDEBUG("%s thread is running", current->comm);

	while (1) {
		atomic_set(&jt->gogo, 0);

 		spin_lock_irqsave(&jt->list_lock, flags);

		// jobs loop
		list_for_each_entry_safe(job, safe, &jt->list, job_node) {
			spin_unlock_irqrestore(&jt->list_lock, flags);
			if (dms_job_io_done(job))
				dms_job_run(job);
			dms_flush_defer_bh(cow);
			
			spin_lock_irqsave(&jt->list_lock, flags);
		}

		set_current_state(TASK_INTERRUPTIBLE);
		if(!atomic_read(&jt->gogo)) {
			if (atomic_read(&jt->quit) && list_empty(&jt->list)) {
				spin_unlock_irqrestore(&jt->list_lock, flags);
				break;
			}
			spin_unlock_irqrestore(&jt->list_lock, flags);
			//schedule();

			sec = 30;
			while (1) {
				if (!schedule_timeout(HZ * sec)) {
					dms_dbg_thread_info(cow);
					if (sec < 36000) sec <<= 2;
				} else {
					break;
				}
			}
			continue;
		}
		spin_unlock_irqrestore(&jt->list_lock, flags);
		set_current_state(TASK_RUNNING);
	}


	jt->process_task = 0;
	DMDEBUG("%s thread thread exits", current->comm);

	atomic_set(&jt->quit, 2);

	return 0;
}


/*
 * never return error
 */
void dms_job_create_thread(dms_cow_t *cow)
{
	dms_job_thread_t *jt = &cow->job_thread;

	atomic_set(&jt->quit, 0);
	spin_lock_init(&jt->list_lock);
	INIT_LIST_HEAD(&jt->list);

	jt->process_task = kthread_create(
		dms_job_thread, cow, 
		"dms_%x_%s", cow->cow_id, dms_bdev_name(cow->bdev)
	);
	DMDEBUG("create %s thread %d", jt->process_task->comm, jt->process_task->tgid);
}


void dms_job_delete_thread(dms_cow_t *cow)
{
	int c = 0;
	dms_job_thread_t *jt = &cow->job_thread;

	BUG_ON(!jt->process_task);

	atomic_set(&jt->quit, 1);
	wake_up_process(jt->process_task);

	while (atomic_read(&jt->quit) != 2) {
		schedule_timeout_uninterruptible(HZ);
		c++;
		if (!(c & 7)) {
			DMDEBUG("waiting %d seconds for %s thread to exit", c, jt->process_task->comm);
			dms_dbg_thread_info(cow);
		}
	}
}


/****************
 *
 * cache
 *
 ****************/


/*
 * get misc buffer head. (inc the ref count of bh)
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_misc_init(dms_cow_t *cow)
{
	dms_bh_t *bh;

	bh = dms_bh_sread(cow->bdev, 0);
	if (!bh) {
		dms_io_err(cow->bdev, "init misc cache");
		return -EIO;
	}

	cow->misc.disk = page_address(bh->b_page);
	cow->misc.bh = bh;

	return 0;	
}


void dms_cache_misc_free(dms_cow_t *cow)
{
	if (cow->misc.bh) {
		brelse(cow->misc.bh);
		cow->misc.disk = 0;
		cow->misc.bh = 0;
	}
}


void dms_cache_generic_free(dms_bh_t *bh[], void *disk[], int nr_pages)
{
	int i;

	for (i = 0; i < nr_pages; i++) {
		if (bh[i]) brelse(bh[i]);
		bh[i] = 0;
		disk[i] = 0;
	}
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_generic_init(
	dms_bdev_t *bdev, 
	dms_bh_t *bh[], 
	void *disk[], 
	int nr_pages, 
	int base_page_idx, 
	char *msg)
{
	int i, page_idx;
	dms_bh_t *bh_temp;

	for (i = 0; i < nr_pages; i++) {
		page_idx = base_page_idx + i;

		bh_temp = dms_bh_sread(bdev, page_idx);
		if (!bh_temp) goto bad;

		bh[i] = bh_temp;
		disk[i] = page_address(bh_temp->b_page);
	}

	return 0;

bad:
	dms_cache_generic_free(bh, disk, nr_pages);
	DMERR("init %s cache IO error on %s", msg, dms_bdev_name(bdev));
	return -EIO;
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_vtree_init(dms_orig_t *orig)
{
	uint32 r;
	
	r = dms_cache_generic_init(
		orig->cow->bdev,
		orig->vtree.bh,
		(void*)orig->vtree.disk,
		VTREE_SLOT_SIZE, 
		VERSION_TREE_ZONE_P + VTREE_SLOT_SIZE * orig->slot_id,
		"version tree"
	);

	RET_EXX(r);
	
	dms_vtree_init_auxiliary(orig);

	return 0;
}


void dms_cache_vtree_free(dms_orig_t *orig)
{
	dms_cache_generic_free(
		orig->vtree.bh, (void*)orig->vtree.disk, VTREE_SLOT_SIZE
	);
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_index1_init(dms_orig_t *orig)
{
	return dms_cache_generic_init(
		orig->cow->bdev,
		orig->index1.bh,
		(void*)orig->index1.disk,
		INDEX1_SLOT_SIZE,
		INDEX1_ZONE_P + INDEX1_SLOT_SIZE * orig->slot_id,
		"index1"
	);
}


void dms_cache_index1_free(dms_orig_t *orig)
{
	dms_cache_generic_free(
		orig->index1.bh, (void*)orig->index1.disk, INDEX1_SLOT_SIZE
	);
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_plot_bitmap_init(dms_cow_t *cow)
{
	return dms_cache_generic_init(
		cow->bdev,
		cow->plot_bitmap.bh,
		(void*)cow->plot_bitmap.disk,
		PLOT_BITMAP_ZONE_SIZE,
		PLOT_BITMAP_ZONE_P,
		"plot bitmap"
	);
}


void dms_cache_plot_bitmap_free(dms_cow_t *cow)
{
	dms_cache_generic_free(
		cow->plot_bitmap.bh,
		(void*)cow->plot_bitmap.disk,
		PLOT_BITMAP_ZONE_SIZE
	);
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_cache_chunk_bitmap_init(dms_cow_t *cow)
{
	return dms_cache_generic_init(
		cow->bdev,
		cow->chunk_bitmap.bh,
		(void*)cow->chunk_bitmap.disk,
		CHUNK_BITMAP_ZONE_SIZE,
		CHUNK_BITMAP_ZONE_P,
		"chunk bitmap"
	);
}


void dms_cache_chunk_bitmap_free(dms_cow_t *cow)
{
	dms_cache_generic_free(
		cow->chunk_bitmap.bh,
		(void*)cow->chunk_bitmap.disk,
		CHUNK_BITMAP_ZONE_SIZE
	);
}


/****************
 *
 * cow device
 *
 ****************/


void dms_cow_info(dms_cow_t *cow)
{
	static uint32 c = 0;
	dms_misc_disk_t *misc_disk = cow->misc.disk;

	if (c > 3) return;
	c++;

	DMINFO("cow device %s info:", dms_bdev_name(cow->bdev));
	DMINFO(
		"  device size: %u chunks (%llu sectors)", 
		misc_disk->cow_size_in_chunk, misc_disk->cow_size_in_sect
	);
	DMINFO("  chunk size: %u pages", misc_disk->chunk_size);
	DMINFO("  sector_shift=%u", cow->sector_shift);
}


void dms_cow_mark_invalid(dms_cow_t *cow)
{
	if (!cow->misc.disk->invalid) {
		cow->misc.disk->invalid = 1;
		dms_bh_swrite(cow->misc.bh);
	}
}


/*
 * find out an available cow-id
 *
 * return cow-id if ok
 * return -ENOSPC if error
 */
int dms_cow_find_cowid(void)
{
	int i;
	uint32 bitmap = 0;
	dms_cow_t *cow;

	list_for_each_entry(cow, &dms_cows_lh, cows_ln)
		dms_write_bit(&bitmap, cow->cow_id, 1);

	for (i = 0; i < DMS_MAX_NR_COWS; i++)
		if (!dms_read_bit(&bitmap, i))
			return i;

	return -ENOSPC;
}


void dms_cow_free(dms_cow_t *cow)
{
	if (!list_empty(&cow->cows_ln))
		list_del(&cow->cows_ln);

	dms_job_delete_thread(cow);

	dms_cache_chunk_bitmap_free(cow);
	dms_cache_plot_bitmap_free(cow);
	dms_cache_misc_free(cow);

	kfree(cow);
}


/*
 * alloc and init a cow
 *
 * return a alloced cow if ok
 * return 0 if error
 */
static dms_cow_t* dms_cow_alloc(struct dm_dev *dd_cow)
{
	uint32 cow_id;
	dms_cow_t *cow;

	cow_id = dms_cow_find_cowid();
	if (IS_EXX(cow_id)) goto bad1;

	cow = dms_kmalloc(sizeof(dms_cow_t), "cow");
	memset(cow, 0, sizeof(dms_cow_t));

	cow->cow_id = cow_id;
	cow->bdev = dd_cow->bdev;

	atomic_set(&cow->count, 0);
	INIT_LIST_HEAD(&cow->cows_ln);
	INIT_LIST_HEAD(&cow->defer_bh_head);
	spin_lock_init(&cow->wake_iocount_lock);

	if (dms_cache_misc_init(cow)) goto bad2;
	if (dms_cache_plot_bitmap_init(cow)) goto bad2;
	if (dms_cache_chunk_bitmap_init(cow)) goto bad2;
	dms_job_create_thread(cow);

	//sector_nr >> sector_shift = chunk_nr
	cow->sector_shift = sector_shift(cow->misc.disk->chunk_size);
	cow->next_alloc_chunk =  cow->misc.disk->cow_size_in_chunk - 1;

	dms_cow_info(cow);

	list_add(&cow->cows_ln, &dms_cows_lh);

	return cow;

bad2:
	dms_cow_free(cow);
bad1:
	return 0;
}


/*
 * find a cow by dm_dev in the dsm_cows_lh list
 *
 * return the non-zeo cow pointer if ok
 * return 0 if not found
 */
static dms_cow_t* dms_cow_find(struct dm_dev *dd_cow)
{
	dms_cow_t *cow;

	list_for_each_entry(cow, &dms_cows_lh, cows_ln)
		if (cow->bdev == dd_cow->bdev)
			return cow;

	return 0;
}

	
void dms_cow_put(dms_cow_t *cow)
{
	if (atomic_dec_and_test(&cow->count)) 
		dms_cow_free(cow);
}


/*
 * find out cow by @dd_cow, otherwise alloc a new one
 *
 * return the non-zero cow pointer if ok
 * return 0 if error
 */
static dms_cow_t* dms_cow_get(struct dm_dev *dd_cow)
{
	dms_cow_t *cow;

	cow = dms_cow_find(dd_cow);

	if (!cow)
		cow = dms_cow_alloc(dd_cow);

	if (cow) 
		atomic_inc(&cow->count);

	return cow;
}


/*
 * find slot in @cow by @orig_dev_id of orig device
 *
 * return the slot ID if found (which starts from 0)
 * return EINVAL if not found
 */
int dms_cow_find_slot(dms_cow_t *cow, char* orig_dev_id)
{
	int i;
	dms_misc_disk_t *misc_disk;

	misc_disk = cow->misc.disk;

	for (i = 0; i < MAX_NR_ORIG_DEVS; i++)
		if (!strncmp(misc_disk->orig_dev_ids[i], orig_dev_id, ORIG_DEV_ID_LEN))
			return i;

	DMERR("origin device ID %s not found in %s", orig_dev_id, dms_bdev_name(cow->bdev));

	return -EINVAL;
}


/****************
 *
 * orig
 *
 ****************/


void dms_orig_wait_job_done(dms_orig_t *orig)
{
	uint32 c = 0;
	while (!list_empty(&orig->rollback_info.job.job_node) ||
		!list_empty(&orig->clean_info.job.job_node))
	{
		schedule_timeout_uninterruptible(HZ);
		c++;
		if (!(c & 7)) {
			DMDEBUG("waiting %d seconds for jobs of origin %s done", c, dms_bdev_name(orig->bdev));
		}
	}
}


void dms_orig_free(dms_orig_t *orig)
{
	orig->freeing = 1;

	if (!list_empty(&orig->origs_ln))
		list_del(&orig->origs_ln);

	dms_orig_wait_job_done(orig);
	dms_cow_put(orig->cow);

	dms_cache_vtree_free(orig);
	dms_cache_index1_free(orig);

	kfree(orig);
}


/*
 * find out the orig by dd_orig, otherwise alloc a new one
 *
 * return orig if ok
 * return 0 if error
 */
static dms_orig_t* dms_orig_alloc(struct dm_dev *dd_orig, struct dm_dev *dd_cow, char* orig_dev_id)
{
	dms_orig_t *orig;
	dms_cow_t *cow;

	cow = dms_cow_get(dd_cow);
	if (!cow) {
		DMERR("init cow %s error", dms_bdev_name(dd_cow->bdev));
		goto bad1;
	}

	orig = dms_kmalloc(sizeof(dms_orig_t), "orig");
	memset(orig, 0, sizeof(dms_orig_t));

	orig->cow = cow;
	orig->bdev = dd_orig->bdev;
	orig->orig_size_in_sect = dms_bdev_size_sect(dd_orig->bdev);
	orig->max_chunk_nr = orig->orig_size_in_sect >> cow->sector_shift;
	orig->max_index1_idx = orig->max_chunk_nr >> 10;
	orig->slot_id = dms_cow_find_slot(orig->cow, orig_dev_id);

	if (IS_EXX(orig->slot_id)) goto bad2;

	init_MUTEX(&orig->orig_lock);
	INIT_LIST_HEAD(&orig->origs_ln);

	dms_clean_info_init(&orig->clean_info);
	dms_rollback_info_init(&orig->rollback_info);

	if (dms_cache_vtree_init(orig)) goto bad2;
	if (dms_cache_index1_init(orig)) goto bad2;

	list_add(&orig->origs_ln, &dms_origs_lh);

	dms_job_plug(orig->cow, &(orig->clean_info.job));
	dms_job_wake_up(orig->cow);

	return orig;

bad2:
	dms_orig_free(orig);
bad1:
	return 0;
}


/*
 * find orig in origs_lh list
 *
 * return orig if found
 * return 0 if not
 */
dms_orig_t* dms_orig_find(struct dm_dev *dd_orig)
{
	dms_orig_t *orig;

	list_for_each_entry(orig, &dms_origs_lh, origs_ln) {
		if (orig->bdev == dd_orig->bdev) 
			return orig;
	}

	return 0;	
}


/*
 * find or alloc an orig, increase the ref count
 *
 * return orig if ok
 * return 0 if error
 */
dms_orig_t* dms_orig_get(struct dm_dev *dd_orig, struct dm_dev *dd_cow, char* orig_dev_id)
{
	dms_orig_t *orig;

	orig = dms_orig_find(dd_orig);

	if (!orig) 
		orig = dms_orig_alloc(dd_orig, dd_cow, orig_dev_id);

	if (orig) 
		atomic_inc(&orig->count);

	return orig;
}


void dms_orig_put(dms_orig_t *orig)
{
	if (atomic_dec_and_test(&orig->count)) 
		dms_orig_free(orig);
}


/****************
 *
 * device-mapper device
 *
 ****************/


void dms_dmd_free(dms_dm_dev_t *dmd)
{
	if (!list_empty(&dmd->dmds_ln))
		list_del(&dmd->dmds_ln);
	dms_orig_put(dmd->orig);
	kfree(dmd);
}


/*
 * alloc dm_dev
 *
 * return dm_dev if ok
 * return 0 if error
 */
dms_dm_dev_t*  dms_dmd_alloc(struct dm_target *ti, dms_parsed_args_t *pargs)
{
	int i, j;
	uint16 ver_nr;
	dms_orig_t *orig;
	dms_dm_dev_t *dmd;

	orig = dms_orig_get(pargs->dd_orig, pargs->dd_cow, pargs->orig_dev_id);
	if (!orig) goto bad1;

	ver_nr = 0;

	// cow, not orig
	if (pargs->tag) {
		ver_nr = dms_vtree_get_version_by_tag(orig, pargs->tag);
		if (!ver_nr) {
			DMERR("(%u,%s,%s) not found", 
				pargs->tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
			goto bad2;
		}
	}
	
	dmd = dms_kmalloc(sizeof(dms_dm_dev_t), "dm_dev");
	memset(dmd, 0, sizeof(dms_dm_dev_t));
	INIT_LIST_HEAD(&dmd->dmds_ln);

	dmd->tag = pargs->tag;
	atomic_set(&dmd->ver, ver_nr);
	dmd->orig = orig;
	dmd->dd_orig = pargs->dd_orig;
	dmd->dd_cow = pargs->dd_cow;
	dmd->name = dms_dm_name(ti);

	// bio group hash
	for (i = 0; i < 3; i++) {
		for (j = 0; j < BIO_GROUP_HASH_LEN; j++)
			INIT_LIST_HEAD(&dmd->bio_group_hash[i][j]);
	}
	
	list_add(&dmd->dmds_ln, &dms_dmds_lh);	

	return dmd;

bad2:
	dms_orig_put(orig);
bad1:
	return 0;
}


/****************
 *
 * construct / destruct
 *
 ****************/


void dms_format_info(dms_misc_disk_t *dms_misc, char *cow_dev_name)
{
	DMINFO("cow device %s format info:", cow_dev_name);
	DMINFO("  dev size: %u chunks (%llu sectors)", 
		dms_misc->cow_size_in_chunk, dms_misc->cow_size_in_sect
	);
	DMINFO("  chunk size: %u pages", dms_misc->chunk_size);
	DMINFO("  plot zone end: %u chunk", dms_misc->plot_zone_end);
	DMINFO("  chunk zone start: %u chunk", dms_misc->chunk_zone_start);
}


/*
 * generic format(zero) function
 *
 * bitmap: makr plot 0 as used
 * version tree: init birthday of orig
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_format_generic(struct dm_dev *dd_cow, char *name, int base_page_idx, int nr_pages)
{
	int i, r;
	dms_bh_t *bh;

	for (i = 0; i < nr_pages; i++) {		
		bh = dms_bh_get(dd_cow->bdev, i + base_page_idx);

		dms_zero_page(bh->b_page);

		// mark plot 0 as full. plot nr 0 is used as error returning value
		if (i == 0 && dms_strequ(name, "bitmap")) {
			uint64 *p = page_address(bh->b_page);
			*p = PLOT_BMP_FULL;
		}

		// init birthday of orig
		if (i == 0 && dms_strequ(name, "version tree")) {
			dms_vtree_disk_t *disk = page_address(bh->b_page);
			disk->birthday = dms_get_time();
			strcpy(disk->memo, "ORIGIN");
		}

		r = dms_bh_swrite(bh);

		brelse(bh);

		if (r) {
			DMERR("format %s IO error on %s", name, dms_bdev_name(dd_cow->bdev));
			return r;
		}
	}

	return 0;	
}


/*
 * format(zero) index1 slot
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_format_index1(struct dm_dev *dd_cow, int slot_id)
{
	int base_page_idx;

	base_page_idx = INDEX1_ZONE_P + INDEX1_SLOT_SIZE * slot_id;
	return dms_format_generic(dd_cow, "index1", base_page_idx, INDEX1_SLOT_SIZE);
}


/*
 * format(zero) version tree slot
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_format_vtree(struct dm_dev *dd_cow, int slot_id)
{
	int base_page_idx;

	base_page_idx = VERSION_TREE_ZONE_P + VTREE_SLOT_SIZE * slot_id;
	return dms_format_generic(dd_cow, "version tree", base_page_idx, VTREE_SLOT_SIZE);
}


/*
 * format(zero) plot and chunk bitmap
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_format_bitmap(struct dm_dev *dd_cow)
{
	int nr_pages;

	nr_pages = INDEX1_ZONE_P - PLOT_BITMAP_ZONE_P;

	return dms_format_generic(dd_cow, "bitmap", PLOT_BITMAP_ZONE_P, nr_pages);
}


/*                    
 * return 0 if ok
 * return EXX if error
 */
int dms_format_misc(dms_parsed_args_t *pargs)
{
	int r;
	dms_bh_t *bh;
	uint32 chunk_size;
	struct block_device *bdev;
	dms_misc_disk_t *misc_disk;

	bdev = pargs->dd_cow->bdev;

	chunk_size = dms_calc_chunk_size(pargs);
	if (!chunk_size) {
		r = -EINVAL;
		goto out1;
	}

	// read disk
	bh = dms_bh_sread(bdev, 0);
	if (!bh) {
		dms_io_err(bdev, "format misc");
		r = -EIO;
		goto out1;
	}
	misc_disk = page_address(bh->b_page);

	// argument check
	if (!pargs->force_format_cow &&
		!strncmp(misc_disk->magic, MAGIC_WORD, MAGIC_WORD_LEN))
	{
		DMERR("%s was formated. use 'F' option to Force format",dms_bdev_name(bdev));
		r = -EINVAL;
		goto out2;
	}


	// init misc	
	memset(misc_disk, 0, sizeof(dms_misc_disk_t));

	strncpy(misc_disk->magic, MAGIC_WORD, MAGIC_WORD_LEN);
	strncpy(misc_disk->version, COW_VERSION, COW_VERSION_LEN);

	misc_disk->cow_size_in_sect = dms_bdev_size_sect(bdev);
	misc_disk->chunk_size = chunk_size;
	misc_disk->cow_size_in_chunk = misc_disk->cow_size_in_sect >> sector_shift(chunk_size);
	misc_disk->plot_zone_end = PLOT_ZONE_P / chunk_size;
	misc_disk->chunk_zone_start = misc_disk->cow_size_in_chunk - 1;

	// writeback
	r = dms_bh_swrite(bh); 
	if (r) {
		dms_io_err(bdev, "format misc");
		goto out2;
	}

	dms_format_info(misc_disk, dms_bdev_name(pargs->dd_cow->bdev));
	r = 0;

out2:
	brelse(bh);
out1:
	return r;
}


/*
 * return 0 if ok
 * return EXX if error
 */
int dms_format_cow(dms_parsed_args_t *pargs)
{
	uint32 r;

	r = dms_format_misc(pargs);
	RET_EXX(r);

	r = dms_format_bitmap(pargs->dd_cow);
	
	return r;
}


/*
 * check necessity of bind option in pargs
 * bind and format orig metadata if need
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_bind_orig_to_cow(dms_parsed_args_t *pargs)
{
	int r, slot_id;
	dms_bh_t *bh;
	struct page *page;
	dms_misc_disk_t *misc_disk;
	struct dm_dev *dd_orig = pargs->dd_orig; 
	struct dm_dev *dd_cow = pargs->dd_cow;

	// read misc from disk
	bh = dms_bh_sread(dd_cow->bdev, 0);
	if (!bh) {
		dms_io_err(dd_cow->bdev, "bind");
		r = -EIO;
		goto out0;
	}
	page = bh->b_page;
 	misc_disk = page_address(page);

	// not formated yet?
	if (strncmp(misc_disk->magic, MAGIC_WORD, MAGIC_WORD_LEN)) {
		DMERR("%s is not formated. use 'f' option", pargs->cow_dev_name);
		r = -EINVAL;
		goto out1;
	}

	// find orig
	for (slot_id = 0; slot_id < MAX_NR_ORIG_DEVS; slot_id++) {
		if (!strncmp(misc_disk->orig_dev_ids[slot_id], pargs->orig_dev_id, ORIG_DEV_ID_LEN))
			break;
	}

	// found: orig was already binded to cow
	if (slot_id < MAX_NR_ORIG_DEVS) {
		if (pargs->bind)
			DMWARN("%s is binded to %s. omit 'b' option", 
				dms_bdev_name(dd_orig->bdev), 
				dms_bdev_name(dd_orig->bdev)
			);
		r = 0;
		goto out1;
	}

	// not found: orig is not binded to cow yet
	if (!pargs->bind) {
		DMERR("%s is not binded to %s. use 'b' option",
			dms_bdev_name(dd_orig->bdev), 
			dms_bdev_name(dd_orig->bdev)
		);
		r = -EINVAL;
		goto out1;
	}

	// find an available slot
	for (slot_id = 0; slot_id < MAX_NR_ORIG_DEVS; slot_id++)
		if (!misc_disk->orig_dev_ids[slot_id][0])
			break;

	// no available slot
	if (slot_id == MAX_NR_ORIG_DEVS) {
		DMERR("no available slot on %s", 	dms_bdev_name(dd_cow->bdev));
		r = -EINVAL;
		goto out1;
	}

	// bind orig to cow
	strncpy(misc_disk->orig_dev_ids[slot_id], pargs->orig_dev_id, ORIG_DEV_ID_LEN);

	r = dms_format_index1(dd_cow, slot_id);
	if (r) goto out1;

	r = dms_format_vtree(dd_cow, slot_id);
	if (r) goto out1;

	r = dms_bh_swrite(bh);
	if (r)
		dms_io_err(dd_cow->bdev, "bind");

out1:
	brelse(bh);
out0:
	return r;
}


/*
 * do some checks, bind, format
 *
 * return 0 if ok
 * return EXX if error
 */
int dms_construct_pre(dms_parsed_args_t *pargs)
{
	int r;

	//can NOT format cow device if cow device is in use
	if ((pargs->format_cow || pargs->force_format_cow) && dms_cow_find(pargs->dd_cow)) {
		DMERR("format error, %s is in use", pargs->cow_dev_name);
		r = -EBUSY;
		goto out;
	}

	// can NOT bind orig device to cow device if orig device is in use
	if (pargs->bind && dms_orig_find(pargs->dd_orig)) {
		DMERR("bind error, %s is in use", pargs->orig_dev_name);
		r = -EBUSY;
		goto out;
	}

	r = set_blocksize(pargs->dd_cow->bdev, 4096);
	if (r) goto out;
	
	// format cow device
	if (pargs->format_cow || pargs->force_format_cow)
		if ((r = dms_format_cow(pargs))) {
			DMERR("format %s error", pargs->cow_dev_name);
			goto out;
		}

	// bind orig device to cow device
	r = dms_bind_orig_to_cow(pargs);
	if (r) goto out;

	// check repeated create device
	if (dms_dmd_find_by_dd(pargs->dd_orig, pargs->dd_cow, pargs->tag)) {
		DMERR("(%u,%s,%s) is in use", pargs->tag, pargs->orig_dev_name, pargs->cow_dev_name);
		r = -EINVAL;
		goto out;
	}

	r = 0;

out:
	return r;
}


/****************
 *
 * target haldlers
 *
 ****************/


int dms_status(struct dm_target *ti, status_type_t type, char *result, unsigned maxlen)
{
	int i;
	int sz = 0;
	dms_orig_t *orig;
	dms_cow_t *cow;
	dms_dm_dev_t *dmd, *dmd_find;
	dms_vtree_aux_t *aux;

	dmd = (dms_dm_dev_t*)ti->private;
	orig = dmd->orig;
	cow = orig->cow;
	aux = orig->vtree.auxiliary;

	result[0] = 0;

	LOCK_ORIG();
	
	switch (type) {
	case STATUSTYPE_INFO:
		DMEMIT("\n## orig cow chunk-size available-chunks total-chunks cow-invalid used-versions total-versions clean-job, rollback-job\n");
		DMEMIT("#  %s %s %s %s %u %u %u %d %d %d %d %d\n", 
			dms_bdev_name(orig->bdev), 
			dms_bdev_mm(orig->bdev), 
			dms_bdev_name(cow->bdev), 
			dms_bdev_mm(cow->bdev),
			cow->misc.disk->chunk_size, 
			dms_chunk_real_unused_nr(cow), 
			cow->misc.disk->cow_size_in_chunk,
			cow->misc.disk->invalid,
			dms_vtree_used_versions(orig),
			MAX_NR_OF_VERSIONS,
		        !list_empty(&orig->clean_info.job.job_node),
			!list_empty(&orig->rollback_info.job.job_node)
		);

		DMEMIT("## R version father-version tag birthday nr_excep dm_dev memo\n");
		for (i = 0; i < MAX_NR_OF_VERSIONS; i++) {
			if (!i || VTREE_DISK(i).tag) {
				DMEMIT("#");

				if (dmd->tag == VTREE_DISK(i).tag)
					DMEMIT("* ");
				else
					DMEMIT("  ");

				if (i && orig->rollback_info.tag == i)
					DMEMIT("R ");
				else
					DMEMIT(". ");

				DMEMIT("%u %u %u %u %u ", 
					i, VTREE_DISK(i).father, VTREE_DISK(i).tag,
					VTREE_DISK(i).birthday, VTREE_DISK(i).nr_exceptions
				);

				dmd_find = dms_dmd_find_by_dd(dmd->dd_orig, dmd->dd_cow, VTREE_DISK(i).tag);
				if (dmd_find)
					DMEMIT("%s ", dmd_find->name);
				else
					DMEMIT(". ");

				DMEMIT("\"%s\"", VTREE_DISK(i).memo);

				DMEMIT("\n");
			}
		}
		break;

	case STATUSTYPE_TABLE:
		break;
	}

	UNLOCK_ORIG();

	return 0;
}


/*
 * return 0 if it's NOT dbg message
 */
int dms_dbg_message(dms_orig_t *orig, unsigned argc, char **argv)
{
	// scan job thread
	if (argc == 1 && dms_strequ(argv[0], "jt")) {
		dms_dbg_scan_job_queue(orig->cow);
		return 1;
	}

	// Dump Plot(plot-nr)
	if (argc == 2 && dms_strequ(argv[0], "dplot")) {  
		dms_dbg_dump_plot(orig, simple_strtol(argv[1], 0, 10));
		return 1;
	}

	// Dump Page(cow-dev-page-nr)
	if (argc == 2 && dms_strequ(argv[0], "dpage")) {  
		dms_dbg_dump_cow_page(orig, simple_strtol(argv[1], 0, 10));
		return 1;
	}	

	// Dump Elist(elist-plot-nr)
	if (argc == 2 && dms_strequ(argv[0], "delist")) {  
		dms_dbg_dump_elist(orig, simple_strtol(argv[1], 0, 10));
		return 1;
	}

	// print vtree
	if (argc == 1 && dms_strequ(argv[0], "vt")) {  
		dms_vtree_print(orig);
		return 1;
	}

	return 0;
}


/*
 * return 0 ok
 * return EXX if error
 */
static int dms_message(struct dm_target *ti, unsigned argc, char **argv)
{
	uint32 r = 0;
	dms_dm_dev_t *dmd;
	dms_orig_t *orig;
	dms_parsed_args_t pargs;

	DMDEBUG("received message  %s form %s", dms_str_args(argc, argv), dms_dm_name(ti));

	dmd = (dms_dm_dev_t*)ti->private;
	orig = dmd->orig;

	if ((r = dms_dbg_message(orig, argc, argv)))
		goto out;

	 // parse args
	r = dms_parse_message_args(argc, argv, &pargs);
	if (r) goto out;

	// take a Snapshot of the Origin
	if (pargs.snap_orig) {
		r = dms_cmd_snap_orig(dmd, &pargs);
		if (IS_EXX(r))
			DMERR("took a snapshot of origin (%s,%s) error",
				dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
		else
			DMINFO("took a snapshot of origin: (%u,%s,%s)", 
				r, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
	}

	// take a Snapshot of a Snapshot
	if (pargs.snap_snap) {
		r = dms_cmd_snap_snap(dmd, &pargs);
		if (IS_EXX(r))
			DMERR("took a snapshot of (%u,%s,%s) error",
				pargs.tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
		else
			DMINFO("took a snapshot of (%u,%s,%s): new tag is %u", 
				pargs.tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev), r
			);
	}

	// Delete Origin
	if (pargs.del_orig) {
		r = dms_cmd_delete_orig(dmd, &pargs);
		if (IS_EXX(r))
			DMERR("delete origin (%s, %s) error",
				dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
	}

	// Delete Snapshot
	if (pargs.del_snap) {
		r = dms_cmd_delete_snap(dmd, &pargs);
		if (IS_EXX(r))
			DMERR("delete (%u,%s,%s) error", 
				pargs.tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
	}

	// Rollback Snapshot
	if (pargs.rollback_snap) {
		r = dms_cmd_rollback_snap(dmd, &pargs);
		if (IS_EXX(r))
			DMERR("rollback (%u,%s,%s) error",
				pargs.tag, dms_bdev_name(orig->bdev), dms_bdev_name(orig->cow->bdev)
			);
	}

out:
	return r;
}


void dms_dbg_dump_group_hash(dms_dm_dev_t *dmd)
{
	dms_map_info_t *mi;
	
	list_for_each_entry(
		mi, 
		&dmd->bio_group_hash[2][BIO_GROUP_HASH_FUN(4)], 
		group_bio_node) 
	{
		printk("%u,%p   ", mi->chunk_dmd, mi);
	}
	printk("----\n");
}
		

void dms_dbg_group_hash_find(dms_dm_dev_t *dmd, int idx, uint32 chunk_dmd)
{
	dms_map_info_t *mi;
	
	list_for_each_entry(
		mi, 
		&dmd->bio_group_hash[idx][BIO_GROUP_HASH_FUN(chunk_dmd)], 
		group_bio_node) 
	{
		if (mi->chunk_dmd == chunk_dmd) {
			DMINFO("read write at same time: idx=%d chunk_dmd=%u", idx, chunk_dmd);
			BUG_ON(1);
		}
	}
}
		

 int dms_map(struct dm_target *ti, struct bio *bio, union map_info *map_context)
{
	int map_type;
	int sector_shift;
	chunk_t chunk_dmd;
	dms_orig_t *orig;
	dms_map_info_t *mi;
	dms_dm_dev_t *dmd;

	dmd = (dms_dm_dev_t*)ti->private;
	orig = dmd->orig;
	sector_shift = orig->cow->sector_shift;

	// a bio can not across 2 chunks
	BUG_ON(
		RIGHTBITS64(bio->bi_sector, sector_shift) + bio_sectors(bio) > 
		(orig->cow->misc.disk->chunk_size << 3)
	);
 
	LOCK_ORIG();

	map_type = (bio_data_dir(bio) ? 2 : 0) |(atomic_read(&dmd->ver) ? 1 : 0);
	chunk_dmd = bio->bi_sector >> dmd->orig->cow->sector_shift;

	if (map_type == DMS_MAP_TYPE_RO) {
		// map reading orig immediately
		dms_map_read_orig(bio, dmd);
		goto out; 
	}

	mi = dms_map_group_bio(dmd, bio, chunk_dmd, map_type);
	if (mi) goto out;

	mi = dms_kmalloc(sizeof(dms_map_info_t), "map_info");

	dms_map_info_init(mi, dmd, bio, chunk_dmd, map_type);

	dms_map_group_enhash_mi(mi);

	dms_job_plug(dmd->orig->cow, &mi->job);

	dms_job_wake_up(dmd->orig->cow);

out:
	map_context->ptr = (map_type == DMS_MAP_TYPE_WS) ? mi : 0;
	UNLOCK_ORIG();
	return DM_MAPIO_SUBMITTED;
}



/*
 * this function can prevent from that the size of bio passed to dms_map() too large
 *
 * @bvm: a copy of data of the bio where @biovec will add  into
 * @biovec: new bvec item will be added into bio
 *
 * return the number of byte can be added into my chunk
 */
int  dms_merge(struct dm_target *ti, struct bvec_merge_data *bvm,
			    struct bio_vec *biovec, int max_size)
{
	int x, y;
	int sector_shift, chunk_size;
	dms_dm_dev_t *dmd;

	dmd = (dms_dm_dev_t*)ti->private;
	sector_shift = dmd->orig->cow->sector_shift;
	chunk_size = dmd->orig->cow->misc.disk->chunk_size; // in page

	/*
	 *  +--------- chunk_size << 12 -------+
	 *  +------------+-------------+-------+
	 *  +-- x << 9 --+-- bi_size --+-- y --+
	 */
	x = RIGHTBITS64(bvm->bi_sector, sector_shift);
	y =  (chunk_size << 12) - (x << 9) - bvm->bi_size;

	if (y<0) y = 0;

	return y;
}


static void dms_destruct(struct dm_target *ti)
{
	dms_dm_dev_t *dmd;
	struct dm_dev *dd_orig, *dd_cow;
	
	dmd = (dms_dm_dev_t*)ti->private;
	
	dd_orig = dmd->dd_orig;
	dd_cow = dmd ->dd_cow;

	LOCK_G_LISTS();
	dms_dmd_free(dmd);
	UNLOCK_G_LISTS();

	dm_put_device(ti, dd_orig);
	dm_put_device(ti, dd_cow);
}


static int dms_construct(struct dm_target *ti, unsigned int argc, char **argv)
{
	int r;
	dms_dm_dev_t *dmd;
	dms_parsed_args_t pargs;  // parsed args

	LOCK_G_LISTS();

	r = -EINVAL;

	if (dms_parse_construct_args(argc, argv, &pargs)) goto out;

	if (dms_get_device(ti, pargs.orig_dev_name, &pargs.dd_orig)) goto out;
	if (dms_get_device(ti, pargs.cow_dev_name, &pargs.dd_cow)) goto bad1;

	r = dms_construct_pre(&pargs);
	if (r) goto bad2;

	dmd = dms_dmd_alloc(ti, &pargs);
	if (!dmd) {
		DMERR("alloc dmd error");
		r = -EINVAL;
		goto bad2;
	}

	ti->private = dmd;
	ti->split_io = dmd->orig->cow->misc.disk->chunk_size << 3;
	r = 0;
	goto out;

bad2:
	dm_put_device(ti, pargs.dd_cow);
bad1:
	dm_put_device(ti, pargs.dd_orig);
out:
	UNLOCK_G_LISTS();
	return r;
}


int dms_end_io(struct dm_target *ti, struct bio *bio, int error, union map_info *map_context)
{
	dms_map_info_t *mi;

	mi = map_context->ptr;

	if (mi && mi->map_type == DMS_MAP_TYPE_WS)
		dms_map_ws_wake(mi);

	return 0;	
}


struct target_type dms_target = 
{
	.name    = "dm-snap-mv",
	.version = {1, 1, 0},
	.module  = THIS_MODULE,
	.map     = dms_map,
	.message = dms_message,
	.merge   = dms_merge,
	.status  = dms_status,
	.ctr     = dms_construct,
	.dtr     = dms_destruct,
	.end_io  = dms_end_io
};


static int __init dms_mod_init(void)
{
	int r;

	r = dm_register_target(&dms_target);
	if (r) {
		DMERR("register dm-snap-mv target error");
		return r;
	}

	dms_list_kmem_cache = kmem_cache_create("dms_list", sizeof(dms_list_t), 0, 0, 0);
	return 0;
}


static void __exit dms_mod_exit(void)
{
	kmem_cache_destroy(dms_list_kmem_cache);
	
	dm_unregister_target(&dms_target);
}


module_init(dms_mod_init);
module_exit(dms_mod_exit);
MODULE_LICENSE("GPL");
