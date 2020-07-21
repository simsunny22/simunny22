#include <stdbool.h>
//提前引用
typedef struct bplus_node_s* bplus_node_pt;

typedef struct node_queue_s* node_queue_pt;
typedef struct node_queue_s {
        int max;
        int start;
        int end;
        bplus_node_pt* list;
} node_queue_t;


node_queue_pt init_node_queue(int _max);
void finish_node_queue(node_queue_pt queue);
void push_node_queue(node_queue_pt queue, bplus_node_pt node);
int pop_node_queue(node_queue_pt queue, bplus_node_pt* node);
bool queue_is_empty(node_queue_pt queue);
