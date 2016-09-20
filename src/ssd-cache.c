#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <unistd.h>

#include "ssd-cache.h"
#include "smr-simulator/smr-simulator.h"
#include "ssd_buf_table.h"
#include "strategy/clock.h"
#include "strategy/lru.h"

static volatile void* flushSSDBuffer(SSDBufferDesc *ssd_buf_hdr);
static SSDBufferDesc * SSDBufferAlloc(SSDBufferTag ssd_buf_tag, bool *found);
static void * initStrategySSDBuffer(SSDEvictionStrategy strategy);
static SSDBufferDesc * getSSDStrategyBuffer(SSDEvictionStrategy strategy);
static void * hitInSSDBuffer(SSDBufferDesc * ssd_buf_hdr, SSDEvictionStrategy strategy);

/*
 * init buffer hash table, strategy_control, buffer, work_mem
 */
void initSSDBuffer()
{
	initSSDBufTable(NSSDBufTables);

	ssd_buffer_strategy_control = (SSDBufferStrategyControl *) malloc(sizeof(SSDBufferStrategyControl));
    ssd_buffer_strategy_control->n_usedssd = 0;
	ssd_buffer_strategy_control->first_freessd = 0;
	ssd_buffer_strategy_control->last_freessd = NSSDBuffers - 1;

	ssd_buffer_descriptors = (SSDBufferDesc *) malloc(sizeof(SSDBufferDesc)*NSSDBuffers);
	SSDBufferDesc *ssd_buf_hdr;
	long i;
	ssd_buf_hdr = ssd_buffer_descriptors;
	for (i = 0; i < NSSDBuffers; ssd_buf_hdr++, i++) {
		ssd_buf_hdr->ssd_buf_flag = 0;
		ssd_buf_hdr->ssd_buf_id = i;
		ssd_buf_hdr->next_freessd = i + 1;
	}
	ssd_buffer_descriptors[NSSDBuffers - 1].next_freessd = -1;

    initStrategySSDBuffer(EvictStrategy);
}

static volatile void* flushSSDBuffer(SSDBufferDesc *ssd_buf_hdr)
{
	char	ssd_buffer[SSD_BUFFER_SIZE];
	int 	returnCode;

	returnCode = pread(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
	if(returnCode < 0) {            
		printf("[ERROR] flushSSDBuffer():-------read from ssd: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
		exit(-1);
	}    
	returnCode = smrwrite(smr_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_tag.offset);
	//turnCode = pwrite(smr_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_tag.offset);
	if(returnCode < 0) {            
		printf("[ERROR] flushSSDBuffer():-------write to smr: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, ssd_buf_hdr->ssd_buf_tag.offset);
		exit(-1);
	}

    return NULL;
}

static SSDBufferDesc * SSDBufferAlloc(SSDBufferTag ssd_buf_tag, bool *found)
{
	SSDBufferDesc *ssd_buf_hdr;
        unsigned long ssd_buf_hash = ssdbuftableHashcode(&ssd_buf_tag);
        long ssd_buf_id = ssdbuftableLookup(&ssd_buf_tag, ssd_buf_hash);

        if (ssd_buf_id >= 0) {
                ssd_buf_hdr = &ssd_buffer_descriptors[ssd_buf_id];
                *found = 1;
                hitInSSDBuffer(ssd_buf_hdr, EvictStrategy);
                return ssd_buf_hdr;
        }

	ssd_buf_hdr = getSSDStrategyBuffer(EvictStrategy);

        unsigned char old_flag = ssd_buf_hdr->ssd_buf_flag;
        SSDBufferTag old_tag = ssd_buf_hdr->ssd_buf_tag;
        if (DEBUG)
                printf("[INFO] SSDBufferAlloc(): old_flag&SSD_BUF_DIRTY=%d\n", old_flag & SSD_BUF_DIRTY);
        if (old_flag & SSD_BUF_DIRTY != 0) {
                flushSSDBuffer(ssd_buf_hdr);
        }
        if (old_flag & SSD_BUF_VALID != 0) {
                unsigned long old_hash = ssdbuftableHashcode(&old_tag);
                ssdbuftableDelete(&old_tag, old_hash);
        }
        ssdbuftableInsert(&ssd_buf_tag, ssd_buf_hash, ssd_buf_hdr->ssd_buf_id);
        ssd_buf_hdr->ssd_buf_flag &= ~(SSD_BUF_VALID | SSD_BUF_DIRTY);
        ssd_buf_hdr->ssd_buf_tag = ssd_buf_tag;
        *found = 0;

        return ssd_buf_hdr;
}

static void * initStrategySSDBuffer(SSDEvictionStrategy strategy)
{
    if (strategy == CLOCK)
        initSSDBufferForClock();
    else if (strategy == LRU)
        initSSDBufferForLRU();
}

static SSDBufferDesc * getSSDStrategyBuffer(SSDEvictionStrategy strategy)
{
	if (strategy == CLOCK)
		return getCLOCKBuffer();
    else if (strategy == LRU)
        return getLRUBuffer();
}

static void * hitInSSDBuffer(SSDBufferDesc * ssd_buf_hdr, SSDEvictionStrategy strategy)
{
    if (strategy == CLOCK)
        hitInCLOCKBuffer(ssd_buf_hdr);
    else if (strategy == LRU)
        hitInLRUBuffer(ssd_buf_hdr);
}

/*
 * read--return the buf_id of buffer according to buf_tag
 */
void read_block(off_t offset, char* ssd_buffer)
{
	void	*ssd_buf_block;
	bool	found = 0;
	int 	returnCode;

	static SSDBufferTag ssd_buf_tag;
        static SSDBufferDesc *ssd_buf_hdr;

	ssd_buf_tag.offset = offset;
        if (DEBUG)
                printf("[INFO] read():-------offset=%lu\n", offset);
        ssd_buf_hdr = SSDBufferAlloc(ssd_buf_tag, &found);
        if (found) {
		returnCode = pread(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
		if(returnCode < 0) {            
			printf("[ERROR] read():-------read from smr: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, offset);
			exit(-1);
		}    
        }
        else {
		returnCode = smrread(smr_fd, ssd_buffer, SSD_BUFFER_SIZE, offset);
		//returnCode = pread(smr_fd, ssd_buffer, SSD_BUFFER_SIZE, offset);
		if(returnCode < 0) {            
			printf("[ERROR] read():-------read from smr: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, offset);
			exit(-1);
		}    
		returnCode = pwrite(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
		if(returnCode < 0) {            
			printf("[ERROR] read():-------write to ssd: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, offset);
			exit(-1);
		}    

        }
        ssd_buf_hdr->ssd_buf_flag &= ~SSD_BUF_VALID;
        ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_VALID;
}

/*
 * write--return the buf_id of buffer according to buf_tag
 */
void write_block(off_t offset, char* ssd_buffer)
{
        void *ssd_buf_block;
        bool found;
	int  returnCode;

	static SSDBufferTag ssd_buf_tag;
        static SSDBufferDesc *ssd_buf_hdr;

	ssd_buf_tag.offset = offset;
        if (DEBUG)
                printf("[INFO] write():-------offset=%lu\n", offset);
        ssd_buf_hdr = SSDBufferAlloc(ssd_buf_tag, &found);
	returnCode = pwrite(ssd_fd, ssd_buffer, SSD_BUFFER_SIZE, ssd_buf_hdr->ssd_buf_id * SSD_BUFFER_SIZE);
	if(returnCode < 0) {            
		printf("[ERROR] write():-------write to ssd: fd=%d, errorcode=%d, offset=%lu\n", ssd_fd, returnCode, offset);
		exit(-1);
	}
        ssd_buf_hdr->ssd_buf_flag |= SSD_BUF_VALID | SSD_BUF_DIRTY;

	printf("===\n");
}
