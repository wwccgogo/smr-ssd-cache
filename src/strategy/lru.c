#include <stdio.h>
#include <stdlib.h>
#include "../ssd-cache.h"
#include "../smr-simulator/smr-simulator.h"
#include "lru.h"

static volatile void* addToLRUHead(SSDBufferDescForLRU *ssd_buf_hdr_for_lru);
static volatile void* deleteFromLRU(SSDBufferDescForLRU *ssd_buf_hdr_for_lru);
static volatile void* moveToLRUHead(SSDBufferDescForLRU *ssd_buf_hdr_for_lru);

/*
 * init buffer hash table, strategy_control, buffer, work_mem
 */
void initSSDBufferForLRU()
{
	ssd_buffer_strategy_control_for_lru = (SSDBufferStrategyControlForLRU *) malloc(sizeof(SSDBufferStrategyControlForLRU));
    ssd_buffer_strategy_control_for_lru->first_lru = -1;
    ssd_buffer_strategy_control_for_lru->last_lru = -1;

	ssd_buffer_descriptors_for_lru = (SSDBufferDescForLRU *) malloc(sizeof(SSDBufferDescForLRU)*NSSDBuffers);
	SSDBufferDescForLRU *ssd_buf_hdr_for_lru;
	long i;
	ssd_buf_hdr_for_lru = ssd_buffer_descriptors_for_lru;
	for (i = 0; i < NSSDBuffers; ssd_buf_hdr_for_lru++, i++) {
		ssd_buf_hdr_for_lru->ssd_buf_id = i;
        ssd_buf_hdr_for_lru->next_lru = -1;
        ssd_buf_hdr_for_lru->last_lru = -1;
	}
}

static volatile void* addToLRUHead(SSDBufferDescForLRU *ssd_buf_hdr_for_lru)
{
    if (ssd_buffer_strategy_control->n_usedssd == 0) {
	printf("00000000\n");
       ssd_buffer_strategy_control_for_lru->first_lru = ssd_buf_hdr_for_lru->ssd_buf_id;
     	        
	ssd_buffer_strategy_control_for_lru->last_lru = ssd_buf_hdr_for_lru->ssd_buf_id;
	printf("DDDDDDfirst_%ld\nDDDDDlast_%ld\n",ssd_buffer_strategy_control_for_lru->first_lru,ssd_buffer_strategy_control_for_lru->last_lru);
        

    } else {
	printf("LPPPPPPPP  %ld\n",ssd_buffer_descriptors_for_lru[ssd_buffer_strategy_control_for_lru->first_lru].ssd_buf_id);
        ssd_buf_hdr_for_lru->next_lru = ssd_buffer_descriptors_for_lru[ssd_buffer_strategy_control_for_lru->first_lru].ssd_buf_id;
        ssd_buf_hdr_for_lru->last_lru = -1;
        ssd_buffer_descriptors_for_lru[ssd_buffer_strategy_control_for_lru->first_lru].last_lru = ssd_buf_hdr_for_lru->ssd_buf_id;
	printf("LAST %ld\n",ssd_buffer_strategy_control_for_lru->last_lru);

	ssd_buffer_strategy_control_for_lru->first_lru = ssd_buf_hdr_for_lru->ssd_buf_id;
    	printf("kkkkkkkkkkkk %ld",ssd_buffer_strategy_control_for_lru->first_lru );
	}
 ssd_buffer_strategy_control->n_usedssd ++;
        
    return NULL;
}

static volatile void* deleteFromLRU(SSDBufferDescForLRU *ssd_buf_hdr_for_lru)
{
    if (ssd_buf_hdr_for_lru->last_lru >= 0) {
        ssd_buffer_descriptors_for_lru[ssd_buf_hdr_for_lru->last_lru].next_lru=ssd_buf_hdr_for_lru->next_lru;
    } else {
		printf("12312321321321 %ld\n",ssd_buf_hdr_for_lru->next_lru);
        ssd_buffer_strategy_control_for_lru->first_lru = ssd_buf_hdr_for_lru->next_lru;
    }
    if (ssd_buf_hdr_for_lru->next_lru >= 0 ) {
       ssd_buffer_descriptors_for_lru[ssd_buf_hdr_for_lru->next_lru].last_lru=ssd_buf_hdr_for_lru->last_lru;
    } else {
	printf("HEREssd_buf%ld\n",ssd_buf_hdr_for_lru->last_lru);
        ssd_buffer_strategy_control_for_lru->last_lru = ssd_buf_hdr_for_lru->last_lru;
    }
	 ssd_buffer_strategy_control->n_usedssd --;
        
    return NULL;
}

static volatile void* moveToLRUHead(SSDBufferDescForLRU *ssd_buf_hdr_for_lru)
{
    deleteFromLRU(ssd_buf_hdr_for_lru);
    addToLRUHead(ssd_buf_hdr_for_lru);

    return NULL;
}

SSDBufferDesc *getLRUBuffer()
{
	SSDBufferDesc *ssd_buf_hdr;
	SSDBufferDescForLRU *ssd_buf_hdr_for_lru;
    
	if (ssd_buffer_strategy_control->first_freessd >=0 ) {
		ssd_buf_hdr = &ssd_buffer_descriptors[ssd_buffer_strategy_control->first_freessd];
		ssd_buf_hdr_for_lru = &ssd_buffer_descriptors_for_lru[ssd_buffer_strategy_control->first_freessd];
		ssd_buffer_strategy_control->first_freessd = ssd_buf_hdr->next_freessd;
		ssd_buf_hdr->next_freessd = -1;
        addToLRUHead(ssd_buf_hdr_for_lru);
	//printf("DDDDDDfirst_%ld\nDDDDDlast_%ld\n",ssd_buffer_strategy_control_for_lru->first_lru,ssd_buffer_strategy_control_for_lru->last_lru);
       
        return ssd_buf_hdr;
    }

    ssd_buf_hdr = &ssd_buffer_descriptors[ssd_buffer_strategy_control_for_lru->last_lru];
	printf("CCCCC%ld\n",ssd_buffer_strategy_control_for_lru->last_lru);
    ssd_buf_hdr_for_lru = &ssd_buffer_descriptors_for_lru[ssd_buffer_strategy_control_for_lru->last_lru];
    moveToLRUHead(ssd_buf_hdr_for_lru);
	printf("BBBBB%ld\n",ssd_buf_hdr->ssd_buf_id);
    return ssd_buf_hdr;
}

void *hitInLRUBuffer(SSDBufferDesc *ssd_buf_hdr)
{
	printf("HIT!!!!!!!!%ld\n",ssd_buf_hdr->ssd_buf_id);
//	printf("HIT!!!!!!!!!!%ld\n",ssd_buffer_descriptors_for_lru);
    moveToLRUHead(&ssd_buffer_descriptors_for_lru[ssd_buf_hdr->ssd_buf_id]);

    return NULL;
}
