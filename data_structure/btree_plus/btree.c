#include "btree.h"
#include "queue.h"

// ============================================
// create
// ============================================
//create bplus tree
int create_bplus_tree(bplus_tree_pt *_tree, int m) {
    bplus_tree_pt tree = (bplus_tree_pt)malloc(sizeof(bplus_tree_t));
    if (tree == NULL) {
        return 0;
    }
    bplus_node_pt node = create_leaf_node(tree->max);
    if (node == NULL) {
	return 0;
    }
    tree->root = node;
    tree->max = m;
    tree->min = m/2;
    *_tree = tree;
    return 1;
}

//create bplus leaf node 
bplus_node_pt create_leaf_node(int degree) {
	int max = degree + 1;  //for node split;
	bplus_node_pt node = (bplus_node_pt)malloc(sizeof(bplus_node_t));
	if (node == NULL) {
	    return NULL;
	}
	
	
	KEY* _keys = (KEY*)malloc(sizeof(KEY) * max);
	VALUE* _datas = (VALUE*)malloc(sizeof(VALUE) * max);
	if (_keys == NULL || _datas == NULL) {
	    free(_keys);
	    free(_datas);
	    free(node);
	    return NULL;
	}
	
	node->keynum = 0;
	node->keys = _keys;
	node->data = _datas;
	node->child = NULL;
	node->parent = NULL;
	node->next = NULL;
	return node;
}

//create bplus index node 
bplus_node_pt create_index_node(int degree) {
	int max = degree +1;
	bplus_node_pt node = (bplus_node_pt)malloc(sizeof(bplus_node_t));
	if (node == NULL) {
	    return NULL;
	}
	
	KEY* _keys = (KEY*)malloc(sizeof(KEY) * max);
	bplus_node_pt* _children = (bplus_node_pt*)malloc(sizeof(bplus_node_pt) * max);
	if (_keys == NULL || _children == NULL) {
	    free(_keys);
	    free(_children);
	    free(node);
	    return NULL;
	}
	
	node->keynum = 0;
	node->keys = _keys;
	node->data = NULL;
	node->child = _children;
	node->parent = NULL;
	node->next = NULL;
	return node;
}
// ============================================
// insert
// ============================================
int insert_bplus_tree(bplus_tree_pt tree, KEY key, VALUE value) {
	int ret, index;
	
	bplus_node_pt node = tree->root;
	/* 查找到叶节点 */
	while (node->child != NULL) {
		ret = binary_search(node->keys, key, 0, node->keynum - 1, &index);
		node = node->child[index];
	}
	ret = binary_search(node->keys, key, 0, node->keynum - 1, &index);
	if (ret == 1) {
	    fprintf(stderr, "[%s][%d] The node is exist!\n", __FILE__, __LINE__);
	    return 0;
	}
	insert_bplus_node(tree, node, key, value, NULL);
	return 1;
}

void insert_bplus_node(bplus_tree_pt tree, bplus_node_pt node, KEY key, VALUE value, bplus_node_pt child) {
	do_insert_bplus_node(node, key, value, child);

	//split node
	if(node->keynum > tree->max) {
		bplus_node_pt split_node = split_bplus_node(node, tree->max);
		//if node is be split, the split node`s child must be rebind parent
		if (node->child != NULL) {
			rebind_parent(split_node);
		}

		if (node->parent == NULL) {
			bplus_node_pt parent = create_index_node(tree->max);
			//node and split_node bind parent
			node->parent = parent;
			split_node->parent= parent;
			//rebind tree root;
			tree->root = parent;
			//insert child_node to parent
			do_insert_bplus_node(parent, node->keys[0], INVAILD_VALUE, node);
			do_insert_bplus_node(parent, split_node->keys[0], INVAILD_VALUE, split_node);
			return;
		} else {
			insert_bplus_node(tree, node->parent, split_node->keys[0], INVAILD_VALUE, split_node);
		}
	}
	return;
}

void do_insert_bplus_node(bplus_node_pt node, KEY key, VALUE value, bplus_node_pt child) {
	if(child == NULL) {
		insert_leaf_node(node, key, value);
	} else {
		insert_index_node(node, key, child);
	}
}

void insert_leaf_node(bplus_node_pt node,  KEY key, VALUE value) {	
	int keynum = node->keynum;
	node->keys[keynum] = key;
	node->data[keynum] = value;
	KEY tmp_key;
	VALUE tmp_val;
	for(int i = node->keynum ; i > 0 ; i--) {
		if(node->keys[i] >  node->keys[i-1]) {
			break;
		}
		tmp_key = node->keys[i];
		node->keys[i] = node->keys[i-1];
		node->keys[i-1] = tmp_key;

		tmp_val = node->data[i];
		node->data[i] = node->data[i-1];
		node->data[i-1] = tmp_val;
	}	
	node->keynum++;
}

void insert_index_node(bplus_node_pt node, KEY key, bplus_node_pt value) {	
	int keynum = node->keynum;
	node->keys[keynum] = key;
	node->child[keynum] = value;
	KEY tmp_key;
	bplus_node_pt tmp_val;
	for(int i = node->keynum ; i > 0 ; i--) {
		if(node->keys[i] >  node->keys[i-1]) {
			break;
		}
		tmp_key = node->keys[i];
		node->keys[i] = node->keys[i-1];
		node->keys[i-1] = tmp_key;

		tmp_val = node->child[i];
		node->child[i] = node->child[i-1];
		node->child[i-1] = tmp_val;
	}	
	node->keynum++;
}
// ============================================
// delete
// ============================================
int delete_bplus_tree(bplus_tree_pt tree, KEY key) {
	int ret, index;
	
	bplus_node_pt node = tree->root;
	/* 查找到叶节点 */
	while (node->child != NULL) {
		ret = binary_search(node->keys, key, 0, node->keynum - 1, &index);
		node = node->child[index];
	}
	ret = binary_search(node->keys, key, 0, node->keynum - 1, &index);
	if (ret == 0) {
	    fprintf(stderr, "[%s][%d] The node is not exist!\n", __FILE__, __LINE__);
	    return 0;
	}
	delete_bplus_node(tree, node, key);
	return 1;
}

void delete_bplus_node(bplus_tree_pt tree, bplus_node_pt node, KEY key) {
	do_delete_bplus_node(node, key);

	if (node->keynum > tree->min) {
		return;
	}

	bplus_node_pt parent = node->parent;
	if(parent == NULL) {
		//means node is root, and not leaf node;
		if (node->child != NULL && node->keynum <= 1) {
			tree->root = node->child[0];
			node->child[0]->parent = NULL;
			release_node(node);
		} 
		return;
	}
	
	int node_index, sibling_index;
	int min_key = node->keys[0];
	int ret = find_sibling(node, min_key, &node_index, &sibling_index);
	if (ret == 0) {
		fprintf(stderr, "[%s][%d] find sibling_index error!\n", __FILE__, __LINE__);
	}
	bplus_node_pt sibling_node = parent->child[sibling_index];
	//in left
	if (sibling_index < node_index) {
		if(parent->child[sibling_index]->keynum > tree->min) {
			mv_left_node(sibling_node, node, node_index);
		} else {
			int del_key = node->keys[0];
			merge_left_node(sibling_node, node);
			delete_bplus_node(tree, parent, del_key);
		}
		
	}
	//in right
	if (sibling_index > node_index) {
		if (parent->child[sibling_index]->keynum > tree->min) {
			mv_right_node(node, sibling_node, sibling_index);
		} else {
			int del_key = sibling_node->keys[0];
			merge_right_node(node, sibling_node);
                        delete_bplus_node(tree, parent, del_key);
		}
	} 
}

void do_delete_bplus_node(bplus_node_pt node, KEY key) {
	if (node->child == NULL) {
		return delete_leaf_node(node, key);
	} else {
		return delete_index_node(node, key); 
	}
}

void delete_leaf_node(bplus_node_pt node, KEY key) {
	int ret, index;
	int keynum = node->keynum;
	ret = binary_search(node->keys, key, 0, keynum - 1, &index);
	if (ret == 0) {
	    fprintf(stderr, "[%s][%d] The node is not exist!\n", __FILE__, __LINE__);
	}
	KEY tmp_key;
	VALUE tmp_val;
	for (int i = index; i < keynum-1; i++) {
		node->keys[i] = node->keys[i+1];
		node->data[i] = node->data[i+1];
	}
	
	memset(node->keys+keynum-1, 0, sizeof(KEY));
	memset(node->data+keynum-1, 0, sizeof(VALUE));
	node->keynum--;
}

void delete_index_node(bplus_node_pt node, KEY key) {
	int ret, index;
	int keynum = node->keynum;
	ret = binary_search(node->keys, key, 0, keynum - 1, &index);
	if (ret == 0) {
	    fprintf(stderr, "[%s][%d] The node is not exist!\n", __FILE__, __LINE__);
	}
	KEY tmp_key;
	VALUE tmp_val;
	for (int i = index; i < keynum-1; i++) {
		node->keys[i] = node->keys[i+1];
		node->child[i] = node->child[i+1];
	}
	
	memset(node->keys+keynum-1, 0, sizeof(KEY));
	memset(node->child+keynum-1, 0, sizeof(bplus_node_pt));
	node->keynum--;
}

void release_node(bplus_node_pt node) {
	if (node->child == NULL) {
		release_leaf_node(node);
	} else {
		release_index_node(node);
	}
}

void release_leaf_node(bplus_node_pt node) {
	free(node->keys);
	free(node->data);
	free(node);
}

void release_index_node(bplus_node_pt node) {
	free(node->keys);
	free(node->child);
	free(node);
}

int find_sibling(bplus_node_pt node, KEY key, int* index, int* sibling) {
	bplus_node_pt parent = node->parent;
	if (parent == NULL) {
		fprintf(stderr, "[%s][%d] parent is null!\n", __FILE__, __LINE__);
		return 0;
	}

	int left, right;
	int keynum = parent->keynum;
	int ret = binary_search(parent->keys, key, 0, keynum - 1, index);
	if (ret == 0) {
		fprintf(stderr, "[%s][%d] key is not find!\n", __FILE__, __LINE__);
		return 0;
	}
	
	if (*index == 0) {
		*sibling = 1;
		return 1;
	}

	if (*index == parent->keynum-1) {
		*sibling = *index-1;
		return 1;
	}

	left  = *index-1;
	right = *index+1;
	*sibling = parent->child[left]->keynum > parent->child[right]->keynum ?left : right; 
	return 1;
}


// move node
void mv_left_node(bplus_node_pt sibling, bplus_node_pt node, int node_index) {
	if (sibling->child == NULL) {
		mv_left_leaf_node(sibling, node, node_index);
	} else {
		mv_left_index_node(sibling, node, node_index);
		//import
		rebind_parent(node);
	}	

}

void mv_right_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index) {
	if (sibling->child == NULL) {
		mv_right_leaf_node(node, sibling, sibling_index);
	} else {
		mv_right_index_node(node, sibling, sibling_index);
		//import
		rebind_parent(node);
	}	
}

void mv_left_leaf_node(bplus_node_pt sibling, bplus_node_pt node, int node_index) {
	for (int i = node->keynum; i > 0; i--) {
		node->keys[i] = node->keys[i-1]; 
		node->data[i] = node->data[i-1]; 
	}
	node->keys[0] = sibling->keys[sibling->keynum-1];
	node->data[0] = sibling->data[sibling->keynum-1];
	node->keynum++;
	sibling->keynum--;
	
	bplus_node_pt parent = node->parent;
	parent->keys[node_index] = node->keys[0];
}

void mv_left_index_node(bplus_node_pt sibling, bplus_node_pt node, int node_index) {
	for (int i = node->keynum; i > 0; i--) {
		node->keys[i] = node->keys[i-1]; 
		node->child[i] = node->child[i-1]; 
	}
	node->keys[0] = sibling->keys[sibling->keynum-1];
	node->child[0] = sibling->child[sibling->keynum-1];
	node->keynum++;
	sibling->keynum--;
	
	bplus_node_pt parent = node->parent;
	parent->keys[node_index] = node->keys[0];
}

void mv_right_leaf_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index) {
	node->keys[node->keynum] = sibling->keys[0]; 
	node->data[node->keynum] = sibling->data[0]; 

	for (int i = 0; i < sibling->keynum-1; i++) {
		sibling->keys[i] = sibling->keys[i+1]; 
		sibling->data[i] = sibling->data[i+1]; 
	}
	node->keynum++;
	sibling->keynum--;
	
	bplus_node_pt parent = node->parent;
	parent->keys[sibling_index] = sibling->keys[0];
}
	

void mv_right_index_node(bplus_node_pt node, bplus_node_pt sibling, int sibling_index) {
	node->keys[node->keynum] = sibling->keys[0]; 
	node->child[node->keynum] = sibling->child[0]; 

	for (int i = 0; i < sibling->keynum-1; i++) {
		sibling->keys[i] = sibling->keys[i+1]; 
		sibling->child[i] = sibling->child[i+1]; 
	}
	node->keynum++;
	sibling->keynum--;
	
	bplus_node_pt parent = node->parent;
	parent->keys[sibling_index] = sibling->keys[0];
}

// merge node
void merge_left_node(bplus_node_pt sibling, bplus_node_pt node) {
	if (sibling->child == NULL) {
		merge_left_leaf_node(sibling, node);	
	} else {
		merge_left_index_node(sibling, node);
		rebind_parent(sibling);
	}
}

void merge_right_node(bplus_node_pt node, bplus_node_pt sibling) {
	if (sibling->child == NULL) {
		merge_right_leaf_node(node, sibling);	
	} else {
		merge_right_index_node(node, sibling);
		rebind_parent(node);
	}
}

void merge_left_leaf_node(bplus_node_pt sibling, bplus_node_pt node) {
	int i , j ;
	for(i = sibling->keynum , j = 0; j < node->keynum; i++, j++){
		sibling->keys[i] = node->keys[j];
		sibling->data[i] = node->data[j];
	}
	sibling->keynum = sibling->keynum + node->keynum;
	sibling->next = node->next;
	release_node(node);
}

void merge_left_index_node(bplus_node_pt sibling, bplus_node_pt node) {
	int i, j;
	for(i = sibling->keynum, j = 0; j < node->keynum; i++, j++) {
		sibling->keys[i] = node->keys[j];
		sibling->child[i] = node->child[j];
	}
	sibling->keynum = sibling->keynum + node->keynum;
	sibling->next = node->next;
	release_node(node);
}

void merge_right_leaf_node(bplus_node_pt node, bplus_node_pt sibling) {
	int i, j ;
	for(i = node->keynum, j = 0;  j < sibling->keynum; i++, j++) {
		node->keys[i] = sibling->keys[j];
		node->data[i] = sibling->data[j];
	}
	node->keynum = node->keynum + sibling->keynum;
	node->next = sibling->next;
	release_node(sibling);
}

void merge_right_index_node(bplus_node_pt node, bplus_node_pt sibling) {
	int i, j;
	for(i = node->keynum, j = 0; j < sibling->keynum; i++, j++) {
		node->keys[i] = sibling->keys[j];
		node->child[i] = sibling->child[j];
	}
	node->keynum = node->keynum + sibling->keynum;
	node->next = sibling->next;
	release_node(sibling);
}
// ============================================
// split
// ============================================
bplus_node_pt split_bplus_node(bplus_node_pt node, int degree) {
	bplus_node_pt split_node = NULL;
	if (node->child == NULL) {
		split_node = split_leaf_node(node, degree);
	} else {
		split_node = split_index_node(node, degree);
	}
	if (node->parent != NULL) {
		split_node->parent = node->parent; 
	}
	return split_node;
}

bplus_node_pt split_leaf_node(bplus_node_pt node, int degree) {
	int mid = node->keynum / 2;
	int len = node->keynum - mid;
	bplus_node_pt leaf_node = create_leaf_node(degree);
	memcpy(leaf_node->keys, node->keys+mid, sizeof(KEY) * len);
	memcpy(leaf_node->data, node->data+mid, sizeof(VALUE) * len);
	memset(leaf_node->keys+mid, 0, sizeof(KEY) * len);
	memset(leaf_node->data+mid, 0, sizeof(VALUE) * len);
	node->keynum = mid;
	leaf_node->keynum = len;
	node->next=leaf_node;
	return leaf_node;
}

bplus_node_pt split_index_node(bplus_node_pt node, int degree) {
	int mid = node->keynum / 2;
	int len = node->keynum - mid;
	bplus_node_pt index_node = create_index_node(degree);
	memcpy(index_node->keys, node->keys+mid, sizeof(KEY) * len);
	memcpy(index_node->child, node->child+mid, sizeof(bplus_node_pt) * len);
	memset(index_node->keys+mid, 0, sizeof(KEY) * len);
	memset(index_node->child+mid, 0, sizeof(bplus_node_pt) * len);
	node->keynum = mid;
	index_node->keynum = len;
	return index_node;
}

// ============================================
// search
// ============================================
int binary_search(KEY* keys, KEY key, int left, int right, int* index) {
	while (left <= right) {
		int mid = (left + right) /2;

		if (key ==  keys[mid]) {
			*index = mid; 
			return 1;
		}

		if (left == right) {
			break;
			
		}

		if (key < keys[mid]) {
			right = mid-1;
			continue;
		}
		
		left = mid+1;
	}
	*index = left;
	return 0;
}

void rebind_parent(bplus_node_pt node) {
	for(int i = 0; i < node->keynum; i++) {
		bplus_node_pt child = node->child[i];
		child->parent = node;
	}
}

bool is_leaf(bplus_node_pt node) {
        return node->child == NULL;
}
// ============================================
// print
// ============================================
void print_node(bplus_node_pt node, int level, int index) {
	if (is_leaf(node)) {
		print_leaf_node(node, level, index);
	} else {
		print_index_node(node, level, index);
	}
}

void print_leaf_node(bplus_node_pt node, int level, int index) {
	printf("++++++++++++++++++++++ \n");
	printf("leaf node, level:%d, index:%d\n", level, index);
	printf("keynum:%d, self:%p, parent:%p, next:%p \n", node->keynum, node, node->parent, node->next);
	for(int i = 0 ; i < node->keynum; i++) {
		printf("key:%d, value:%d \n", node->keys[i], node->data[i]);
	} 
	
	
}

void print_index_node(bplus_node_pt node, int level, int index) {
	printf("++++++++++++++++++++++ \n");
	printf("index node, level:%d, index:%d \n", level, index);
	printf("keynum:%d, self:%p, parent:%p, next:%p \n", node->keynum, node, node->parent, node->next);
	for(int i = 0 ; i < node->keynum; i++) {
		printf("key:%d, child:%p \n", node->keys[i], node->child[i]);
	} 
}

//
//typedef struct node_queue_s* node_queue_pt;
//typedef struct node_queue_s {
//	int max;
//	int start;
//	int end;
//	bplus_node_pt* list;
//} node_queue_t;
//
//node_queue_pt init_node_queue(int _max) {
//	node_queue_pt  queue = (node_queue_pt) malloc (sizeof(bplus_node_t));
//	bplus_node_pt*  _list = (bplus_node_pt*) malloc (sizeof(bplus_node_pt) * _max);
//	queue->max = _max;
//	queue->start = 0;
//	queue->end  = 0;
//	queue->list = _list;
//	return queue;
//}
//
//void finish_node_queue(node_queue_pt queue) {
//	free(queue->list);
//	free(queue);
//}
//
//void push_node_queue(node_queue_pt queue, bplus_node_pt node) {
//	int end = queue->end ;
//	queue->list[end] = node;
//	queue->end = ++end;
//}
//
//int pop_node_queue(node_queue_pt queue, bplus_node_pt* node) {
//	int start =  queue->start;
//	int end   =  queue->end;
//
//	if (start > end) {
//		return -1;
//	}
//	
//	*node = queue->list[start];
//	queue->start = ++start;
//	return 0;
//	
//}
//
//bool queue_is_empty(node_queue_pt queue) {
//	int start =  queue->start;
//	int end   =  queue->end;
//	return start == end;
//}

void print_tree(bplus_tree_pt tree) {
	printf("================= print tree start ============== \n");

	bplus_node_pt root = tree->root;
	if (is_leaf(root)) {
		print_node(root, 0, 0);
		printf("================= print tree end ============== \n");
		return;
	}

	int level_node_num[100];
	memset(level_node_num, 0, 100);
	level_node_num[0] = 1; 
	int pop_level = 0;
	int pop_level_index = 0;

	node_queue_pt queue = init_node_queue(100);
	push_node_queue(queue, root);

	while(!queue_is_empty(queue)) {
		bplus_node_pt node;
		//pop
		pop_node_queue(queue, &node);
		int next_level = pop_level + 1;
		level_node_num[next_level] = level_node_num[next_level] + node->keynum;
		print_node(node, pop_level, pop_level_index);
		pop_level_index++;

		
		//next level
		if (pop_level_index == level_node_num[pop_level]) {
			pop_level++;	
			pop_level_index = 0;
		} 

		if (!is_leaf(node)) {
			for(int i = 0; i < node->keynum; i++) {
				push_node_queue(queue, node->child[i]);
			}
		}
		
	}
	printf("================= print tree end ============== \n");
}


int main () {
	//create
	bplus_tree_pt bplusTree ;
	create_bplus_tree(&bplusTree, DEGREE);

	//insert
	insert_bplus_tree(bplusTree, 1, 1);
	insert_bplus_tree(bplusTree, 2, 2);
	insert_bplus_tree(bplusTree, 3, 3);
	insert_bplus_tree(bplusTree, 4, 4);
	insert_bplus_tree(bplusTree, 5, 5);
	insert_bplus_tree(bplusTree, 6, 6);
	insert_bplus_tree(bplusTree, 7, 7);
	insert_bplus_tree(bplusTree, 8, 8);
	print_tree(bplusTree);


	//delete
	delete_bplus_tree(bplusTree, 8);
	delete_bplus_tree(bplusTree, 7);
	delete_bplus_tree(bplusTree, 6);
	delete_bplus_tree(bplusTree, 5);
	print_tree(bplusTree);
	//printf("\n\n\n==========\n");
	//delete_bplus_tree(bplusTree, 7);




}

