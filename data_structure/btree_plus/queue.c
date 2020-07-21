#include "queue.h"
#include "btree.h"

node_queue_pt init_node_queue(int _max) {
      node_queue_pt  queue = (node_queue_pt) malloc (sizeof(bplus_node_t));
      bplus_node_pt*  _list = (bplus_node_pt*) malloc (sizeof(bplus_node_pt) * _max);
      queue->max = _max;
      queue->start = 0;
      queue->end  = 0;
      queue->list = _list;
      return queue;
}

void finish_node_queue(node_queue_pt queue) {
        free(queue->list);
        free(queue);
}

void push_node_queue(node_queue_pt queue, bplus_node_pt node) {
        int end = queue->end ;
        queue->list[end] = node;
        queue->end = ++end;
}

int pop_node_queue(node_queue_pt queue, bplus_node_pt* node) {
        int start =  queue->start;
        int end   =  queue->end;

        if (start > end) {
                return -1;
        }

        *node = queue->list[start];
        queue->start = ++start;
        return 0;

}

bool queue_is_empty(node_queue_pt queue) {
        int start =  queue->start;
        int end   =  queue->end;
        return start == end;
}


