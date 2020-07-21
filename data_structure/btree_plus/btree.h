#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#define DEGREE 3
#define TYPE_LEAF 0
#define TYPE_INDEX 1

typedef int KEY;
typedef int VALUE;
#define INVAILD_VALUE -1

typedef struct bplus_node_s* bplus_node_pt;
typedef struct bplus_node_s {
    int   keynum;
    KEY   *keys;            /* 主健，最大max个,最小min个, 空间max+1*/
    VALUE *data;            /* 数据，最大max个,最小min个, 空间max+1，内部节点时，data是NULL */
    bplus_node_pt *child;   /* 子节点，最大max+1个，最小min + 1个, 空间max+2，叶子节点时，child是NULL */
    bplus_node_pt parent;
    bplus_node_pt next;     /* 兄弟节点，仅是叶子节点时有值 */
} bplus_node_t;


typedef struct bplus_tree_s* bplus_tree_pt;
typedef struct bplus_tree_s {
    bplus_node_pt root;
    int max;
    int min;
} bplus_tree_t;


//create
int create_bplus_tree(bplus_tree_pt *_tree, int m);
bplus_node_pt create_leaf_node(int degree);
bplus_node_pt create_index_node(int degree);

//insert
int  insert_bplus_tree(bplus_tree_pt tree, KEY key, VALUE value);
void insert_bplus_node(bplus_tree_pt tree, bplus_node_pt node, KEY key, VALUE value, bplus_node_pt child);
void do_insert_bplus_node(bplus_node_pt node, KEY key, VALUE value, bplus_node_pt child);
void insert_leaf_node(bplus_node_pt node,  KEY key, VALUE value);
void insert_index_node(bplus_node_pt node, KEY key, bplus_node_pt value);	

//delete
int  delete_bplus_tree(bplus_tree_pt tree, KEY key);
void delete_bplus_node(bplus_tree_pt tree, bplus_node_pt node, KEY key);
void do_delete_bplus_node(bplus_node_pt node, KEY key);
void delete_leaf_node(bplus_node_pt node, KEY key);
void delete_index_node(bplus_node_pt node, KEY key);

//release
void release_node(bplus_node_pt node);
void release_leaf_node(bplus_node_pt node);
void release_index_node(bplus_node_pt node);

//split
bplus_node_pt split_bplus_node(bplus_node_pt node, int degree);
bplus_node_pt split_leaf_node(bplus_node_pt node, int degree);
bplus_node_pt split_index_node(bplus_node_pt node, int degree);

//find split
int find_sibling(bplus_node_pt node, KEY key, int* index, int* sibling);

//mv
void mv_left_node(bplus_node_pt sibling, bplus_node_pt node, int node_index);
void mv_right_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index);
void mv_left_leaf_node(bplus_node_pt sibling, bplus_node_pt node, int node_index);
void mv_left_index_node(bplus_node_pt sibling, bplus_node_pt node, int node_index);
void mv_right_leaf_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index);
void mv_right_index_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index);

//merge
void merge_left_node(bplus_node_pt sibling, bplus_node_pt node);
void merge_right_node(bplus_node_pt node, bplus_node_pt sibling);
void merge_left_leaf_node(bplus_node_pt sibling, bplus_node_pt node);
void merge_left_index_node(bplus_node_pt sibling, bplus_node_pt node);
void merge_right_leaf_node(bplus_node_pt node, bplus_node_pt sibling);
void merge_right_index_node(bplus_node_pt node, bplus_node_pt sibling);

//privte
bool is_leaf(bplus_node_pt node);

//print
void print_node(bplus_node_pt node, int level, int index);
void print_leaf_node(bplus_node_pt node, int level, int index);
void print_index_node(bplus_node_pt node, int level, int index);

//search
int binary_search(KEY* keys, KEY key, int left, int right, int* index);
void rebind_parent(bplus_node_pt node);

