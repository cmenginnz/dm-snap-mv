/*
 * Copyright (C) 2010 NOVELL, Inc.
 *
 * Module Author: Cong Meng, NOVELL
 *
 * This file is released under the GPL.
 */


/*
 *  0. cow dev layout
 *
 *  +-----------------------------------+ 0
 *  | 1. misc (1MB)                     | 
 *  +-----------------------------------+ PLOT_BITMAP_ZONE_P  (256 page)
 *  | 2. plot bitmap zone   (256 pages) | 
 *  +-----------------------------------+ CHUNK_BITMAP_ZONE_P (512 page)
 *  | 3. chunk bitmap zone  (256 pages) | 
 *  +-----------------------------------+ INDEX1_ZONE_P       (768 page)
 *  | 4. index1 zone         (64 pages) |
 *  +-----------------------------------+ VERSION_TREE_ZONE_P (832 page)
 *  | 5. version tree zone   (40 pages) |
 *  +-----------------------------------+ PLOT_ZONE_P         (872 page)
 *  | 6. plot zone (grow down)          |
 *  |   - index2 plot                   |
 *  |   - elist plot                    |
 *  + - - - - - - - - - - - - - - - - - +
 *  | 7. chunk zone (grow up)           |
 *  +-----------------------------------+ cow_dev_size
 */ 

// zone starting position
#define  PLOT_BITMAP_ZONE_P 256  // in page
#define CHUNK_BITMAP_ZONE_P 512  // in page
#define       INDEX1_ZONE_P 768  // in page
#define VERSION_TREE_ZONE_P 832  // in page
#define         PLOT_ZONE_P 872  // in page

// zone size
#define  PLOT_BITMAP_ZONE_SIZE 256  // in page
#define CHUNK_BITMAP_ZONE_SIZE 256  // in page


/*
 *  1. misc zone
 *
 *  +----------------------------------+ 
 *  | MAGIC_WORD                 (32B) | 
 *  +----------------------------------+ 
 *  | COW_VERSION                (32B) | 
 *  +----------------------------------+ 
 *  | CHUNK_SIZE         (4B, in page) | 
 *  +----------------------------------+ 
 *  | COW_DEV_SIZE_IN_SECT        (8B) | 
 *  +----------------------------------+ 
 *  | COW_DEV_SIZE      (4B, in chunk) | 
 *  +----------------------------------+ 
 *  | ORIG_DEV_IDS          (256B * 8) | 
 *  +----------------------------------+ 
 *  | ORIG_DEV_SECTS          (8B * 8) | 
 *  +----------------------------------+ 
 *  | PLOT_ZONE_SIZE    (4B, in chunk) | 
 *  +----------------------------------+ 
 *  | CHUNK_ZONE_SIZE   (4B, in chunk) | 
 *  +----------------------------------+ 
 *  | CHUNK_ZONE_USED   (4B, in chunk) | 
 *  +----------------------------------+ 
 *  | INVALID                     (4B) | 
 *  +----------------------------------+
 *
 */

#define MAX_NR_ORIG_DEVS 8 

#define MAGIC_WORD "snapshotmv"
#define COW_VERSION "1.0"

#define MAGIC_WORD_LEN 32
#define COW_VERSION_LEN 32
#define ORIG_DEV_ID_LEN 256


/*
 * 2. plot bitmap zone
 * 
 * actually, plot bitmap is 2-bits map.
 * there are 3 status represented by the 2-bits
 *
 * the bits of plot 0 is initialized to full,
 * as 0 is returned as error value in some plot functions.
 */

#define PLOT_BMP_EMPTY 0x0
#define PLOT_BMP_USED  0x1
#define PLOT_BMP_FULL  0x3


/*
 * 3. chunk bitmap zone
 */

// FIXME: write something about: chunk_zone_seek
//        relationship between every bit & every chunk
//        chunk was allocated starting from the end of chunk zone
//        chunks at the beginning of chunk zone is for plot



/*
 *  4. index1 zone
 *
 *  +-----------------------------------+ INDEX1_ZONE_P (3072KB)
 *  | index1 slot 0                     |   INDEX1_SLOT_SIZE (8 page)
 *  +- - - - - - - - - - - - - - - - - -+
 *  | index1 slot 1                     |
 *  +-----------------------------------+
 *  | ...                               | 
 *  +- - - - - - - - - - - - - - - - - -+
 *  | index1 slot 7                     | 
 *  +-----------------------------------+
 *
 *  An index1 slot corresponds to a orig device
 *
 *  An index1 slot is an array, of which each element is an index2 plot number
 */

#define INDEX1_SLOT_SIZE 8  // in page


/*
 *  5. version tree zone
 *
 *  +-----------------------------------+ VERSION_TREE_ZONE_P (887 page)
 *  | vtree slot 0                      |   VTREE_SLOT_SIZE (5 page)
 *  +- - - - - - - - - - - - - - - - - -+
 *  | vtree slot 1                      |
 *  +-----------------------------------+
 *  | ...                               | 
 *  +- - - - - - - - - - - - - - - - - -+
 *  | vtree slot 7                      | 
 *  +-----------------------------------+
 *
 *  A vtree slot corresponds to a orig devices
 *
 *  A vtree slot is an array, of which each element is a dms_vtree_disk_t struct
 *
 *  the maximal number of versions is affected by:
 *      1. the size of elist plot: 4096
 *      2. sizeof(dms_ver_chunk_t): 6
 *      3. vtree slot size must be multiple of page size 4096
 *      4. sizeof(dms_vtree_disk_t): 32
 *  so,
 *      4096 / 6 = 682
 *      682 / (4096 / 32) = 5.xx
 *      4096 / 32 * 5 = 640
 */
#define VTREE_SLOT_SIZE 5   // in page
#define MAX_NR_OF_VERSIONS 640

/*
 * 0x0000     unused tag
 *
 * 0x0001 -+
 * ......  +- User usable tags -+
 * 0xFFFD -+                    |
 *                              +- Live tags
 *                              |
 * 0xFFFE     Ghost tag        -+
 *
 * 0xFFFF     Deleting tag
 */

#define VTREE_TAG_USER      0xFFFD
#define VTREE_TAG_GHOST     0XFFFE
#define VTREE_TAG_DELETING  0xFFFF

#define VTREE_TAG_LIVE      VTREE_TAG_GHOST

#define VTREE_MEMO_LEN 20


/*
 *  6. plot zone
 *  7. chunk zone
 *
 *  +-----------------------------+ PLOT_ZONE_P (872 page)
 *  | plot zone (grow down)       |
 *  |   - index2 plot             |
 *  |   - elist plot              |
 *  + - - - - - - - - - - - - - - |
 *  | chunk zone (grow up)        |
 *  +-----------------------------+ cow_dev_size
 *
 *  plot 0 is wasted because 0 is used as error returning value in some functions
 *  plot 0 is at the 1024 page.
 *  chunk 0 starts at 0 page.
 *  Chunk allocating starts from the end of cow device
 *
 *  plot zone contains 2 kinds of plot:
 *    1. index2 plot:
 *      +---+--------+---+
 *      | I | ...... | I |
 *      +---+--------+---+
 *      I: Index2, point to a elist plot (plot_t)
 *
 *    2. elist(exception list) plot:
 *      +---+---+----+----+-----------+-------------+----------+
 *      | N | 0 | CO | CO | -->...<-- | VC VC VC VC | VC VC VC | 
 *      +---+---+----+----+-----------+-------------+----------+
 *                1    2                O2 6  5  4    O1  2  1
 *      N:   Number of COs (uint16)
 *      0:   (uint16) (Offset of fake CO0)
 *      CO:  C: Chunk_nr of orig (uint32)
 *           O: Offset of exception list (in VC) (uint16)
 *           (COs sorted by C: C1<C2)
 *      VC:  V: Version_nr (uint16)
 *           C: Chunk_nr of cow (uint32)
 *           (VCs sorted by V: V2>V1)
 *
 *  FIXME: write something about the relationship between plot & chunk
 *     ex: plot is ontop of chunk
 *         PLOT_ZONE_SIZE & CHUNK_ZONE_SIZE in misc zone
 *
 *
 */

// size of a plot
#define PLOT_SIZE 4 // in KB

#define DEFAULT_CHUNK_SIZE 1  //in page, 4 bytes

// no limit to this number actually. we just do the test up to 64
#define MAX_CHUNK_SIZE 64  // in page

#define DM_MSG_PREFIX "dm-snap-mv"

#define DMS_MAX_NR_COWS 8




// common type
typedef unsigned char uint8;
typedef short int16;
typedef unsigned short uint16;
typedef int int32;
typedef unsigned int uint32;
typedef unsigned long long  uint64;



typedef uint32 plot_t;
typedef uint32 chunk_t;

/*
 * sizeof(dms_misc_disk_t) should be no more than 4096 now
 */
typedef struct
{
	char magic[MAGIC_WORD_LEN];
	char version[COW_VERSION_LEN];
	uint32 chunk_size;  // in page
	uint64 cow_size_in_sect;  // in sector
	uint32 cow_size_in_chunk; // in chunk
	char orig_dev_ids[MAX_NR_ORIG_DEVS][ORIG_DEV_ID_LEN];
	uint64 orig_dev_sects[MAX_NR_ORIG_DEVS]; 
	chunk_t plot_zone_end;  // in chunk
	chunk_t chunk_zone_start;  // in chunk
	chunk_t chunk_zone_used;  // the nr of chunks allocated by chunk_alloc()
	uint32 invalid;
} __attribute__((packed)) dms_misc_disk_t;


/*
 *  V1
 *  +-V2
 *  +-V3
 *
 *  V[1]={2, 2, 0}         // V1 has 2 children, the 1st one is V2
 *           |
 *         V[2]={0, 0, 3}  // V2 has a brother V3
 *                     |
 *                   V[3]={0, 0, 0}
 */
typedef struct
{
	uint16 nr_children;  // number of children
	uint16 child;  // version NR of the 1st child
	uint16 brother;  // version NR of the next child of father, 0 if none
} __attribute__((packed)) dms_vtree_aux_t;


// disk-layout of vtree slot
// size: 32 Bytes
typedef struct
{
	uint16 father;
	uint16 tag;
	uint32 nr_exceptions;  // the number of exceptions this version owns
	uint32 birthday;
	char memo[VTREE_MEMO_LEN];
} __attribute__((packed)) dms_vtree_disk_t;


typedef struct
{
	uint16 ver;
	// chunk_cow
	uint32 chunk;
} __attribute__((packed)) dms_ver_chunk_t;


typedef struct
{
	// chunk_dmd
	uint32 chunk;
	uint16 offset;
} __attribute__((packed)) dms_chunk_offset_t;
