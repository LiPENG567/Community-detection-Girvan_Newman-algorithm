#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from itertools import permutations
import itertools
import time
import os
import json
import random
import sys

filter_thresh = sys.argv[1]
input_path = sys.argv[2]
out_path_btw = sys.argv[3]
out_path_comm = sys.argv[4]
fliter_t = int(filter_thresh)

start = time.time()
sc = pyspark.SparkContext()

rdd = sc.textFile(input_path)
header = rdd.first()
rdd_d = rdd.filter(lambda a: a!= header)
Myrdd = rdd_d.map(lambda line: (line.split(',')[0], line.split(',')[1])).persist()
user_rddr = Myrdd.groupByKey()
user_busi_dict = user_rddr.map(lambda a: (a[0],list(a[1]))).filter(lambda a: len(a[1])>= fliter_t).map(lambda a: {a[0]:a[1]}).flatMap(lambda a: a.items()).collectAsMap()
user_rdd = user_rddr.map(lambda a: a[0])
# user_df = user_rdd.map(lambda x: (x, )).toDF()

# create edge as dataframe 
def check3(dict1, dict2):
    ins_len = len(set(dict1).intersection(set(dict2)))
    if ins_len >= 7:
        return True
    else:
        return False
edge_rddr = user_rdd.cartesian(user_rdd).filter(lambda a: a[0] != a[1]).filter(lambda a: check3(user_busi_dict[a[0]],user_busi_dict[a[1]]))
m = int((edge_rddr.count())/2)
edge_dict = edge_rddr.groupByKey().map(lambda a: {a[0]:list(a[1])}).flatMap(lambda a: a.items()).collectAsMap()

def adjMatrix(edge_dict):
    A_matrix = set()
    for start_node, end_nodes in edge_dict.items():
        for end_node in end_nodes:
            A_matrix.add(tuple(sorted([start_node, end_node])))
    return A_matrix
A_matrix = adjMatrix(edge_dict)
            

# print(new_rddr.take(5))
vertex = edge_rddr.map(lambda a: list([a[0],a[1]])).flatMap(lambda a: a).distinct().collect()
print(edge_rddr.count())


def bfs(vertex,edge_dict, start):
    ### build BFS first and add parent node as well as level for each node ###
    tree = dict()
    tree[start] = (0,list())
    explored = set()
    queue = [start]
    while queue:
        parent = queue.pop(0)
        explored.add(parent)
        for child in edge_dict[parent]:
#             print(parent, child)
            if child not in explored:
                explored.add(child)
                tree[child] = (tree[parent][0]+1, [parent])
#                 print(tree)
                queue.append(child)
            elif tree[parent][0] +1 == tree[child][0]:
            # check child already in explored, add more parent
                tree[child][1].append(parent)
    return tree
# vertex = ['a','b','c','d']
# edge_dict = {'a':['b','c'], 'b':['d'], 'c':['d'], 'd':['b','c']}

# tree_dict = bfs(vertex,edge_dict, 'a')
# print(tree_dict)

def num_shortest_path (tree_dict):
    num_shortest_dict = dict()
    l_dict = dict()
    for child, le_pa in tree_dict.items():
        l_dict.setdefault(le_pa[0], []).append((child, le_pa[1]))
    for l in range(0, len(l_dict.keys())):
        for (child, parent_list) in l_dict[l]:
            if len(parent_list) > 0:
                num_shortest_dict[child] = sum([num_shortest_dict[parent] for parent in parent_list])
            else:
                num_shortest_dict[child] = 1
    return num_shortest_dict

# print(num_shortest_path(tree_dict))


def extend_dict(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1.keys():
            dict1[key] = float(dict1[key]+value)
        else:
            dict1[key] = value
    return dict1

def traverse_tree(tree_dict,vertex):
    tree_dict = {k: v for k, v in sorted(tree_dict.items(), key=lambda a: -a[1][0])} # from bottom to up
    init_dict = dict.fromkeys(vertex,1)
    shortest_path_dict = num_shortest_path(tree_dict)
    result_dict = dict()
    for key, value in tree_dict.items():
        if len(value[1])>0:
            denom = sum([shortest_path_dict[pa] for pa in value[1]])
            for parent in value[1]:
                new_key = tuple(sorted([key,parent]))
#                 print(temp_key)
                weight = float(float(init_dict[key]) * int(shortest_path_dict[parent]) / denom)
                result_dict[new_key] = weight
                init_dict[parent] = float(init_dict[parent]+weight)
    return result_dict
# print(traverse_tree(tree_dict,vertex))

def combtw(vertex, edge_dict):
    res = dict()
    for node in vertex:
        tree_dict = bfs(vertex,edge_dict, node)
        temp_result_dict = traverse_tree(tree_dict,vertex)
        res = extend_dict(res, temp_result_dict)
    btwness = dict()
    for k, v in res.items():
        btwness[k] = float(float(v)/2)
    result = [(k,v) for k, v in btwness.items()]
    btw_list = sc.parallelize(result).map(lambda a: (tuple(sorted(a[0])), a[1])).map(lambda a: (a[0][0], (a[0][1],a[1]))).sortByKey().map(lambda a: (a[1][1], (a[0],a[1][0]))).sortByKey(ascending=False).map(lambda a: (tuple(a[1]),a[0])).collect()
    return btw_list

btw_list = combtw(vertex, edge_dict)

# btw_list = sc.parallelize(btw_list).map(lambda a: (a[1], tuple(sorted(a[0])))).sortByKey(ascending=False).map(lambda a: (a[1][0], (a[1][1],a[0]))).sortByKey().map(lambda a: ((a[0], a[1][0]), a[1][1])).collect()
# btw_list = sc.parallelize(btw_list).map(lambda a: (tuple(sorted(a[0])), a[1])).map(lambda a: (a[0][0], (a[0][1],a[1]))).sortByKey().map(lambda a: (a[1][1], (a[0],a[1][0]))).sortByKey(ascending=False).map(lambda a: (tuple(a[1]),a[0])).collect()
# sortByKey(ascending=False).map(lambda a: (a[1][0], (a[1][1],a[0]))).sortByKey().map(lambda a: ((a[0], a[1][0]), a[1][1])).collect()


## community detection
#btw_tuple already sorted from high to low, therefore remove from list one by on
# def cut_high_edge(edge_dict, btw_tuple):
#     edge = btw_tuple[0][0]
#     print("I ma 222222222222222222222222222222222222222222222222222222",edge)
#     if edge_dict[edge[0]] is not None:
#         try:
#             edge_dict[edge[0]].remove(edge[1])
#         except ValueError:
#             pass
#     if edge_dict[edge[1]] is not None:
#         try:
#             edge_dict[edge[1]].remove(edge[0])
#         except ValueError:
#             pass 
#     return edge_dict 

def cut_high_edge(edge_dict, btw_tuple):
    b = [a for a in btw_tuple if a[1]==btw_tuple[0][1]]
    list_c = [a[0] for a in b]
#     print("I ma 222222222222222222222222222222222222222222222222222222",list_c)
    while list_c:
        edge = list_c.pop(0)
        if edge_dict[edge[0]] is not None:
            try:
                edge_dict[edge[0]].remove(edge[1])
            except ValueError:
                pass
        if edge_dict[edge[1]] is not None:
            try:
                edge_dict[edge[1]].remove(edge[0])
            except ValueError:
                pass     
    return edge_dict
# new_tree_dict = cut_high_edge(edge_dict, btw_tuple)
        
def dectect_num_comm(vertex, new_tree_dict):
    edge_dict = new_tree_dict
    community = list()
    explored = set()
    one_comm = set()
#     root_node = random.choice(vertex)
    root_node = vertex[random.randint(0,len(vertex)-1)]
    queue = [root_node]
    one_comm.add(root_node)
    while len(explored) != len(vertex):
        while queue:
            parent = queue.pop(0)
            explored.add(parent)
            one_comm.add(parent)
            for child in edge_dict[parent]:
    #             print(parent, child)
                if child not in explored:
                    explored.add(child)
                    one_comm.add(child)
                    queue.append(child)
                    
        community.append(sorted(one_comm))
        one_comm = set()
        if len(vertex) > len(explored):
            queue.append(set(vertex).difference(explored).pop())
    return community

# community = dectect_num_comm(vertex, new_tree_dict)

# def compute_mod(community, n_edge_dict, edge_dict, m):
#     mod_s = 0
#     A = 0
#     for one_comm in community:
#         all_pair = itertools.combinations(list(one_comm),2)
#         for pair in all_pair:
#             ki = len(n_edge_dict[pair[0]])
#             kj = len(n_edge_dict[pair[1]])
#             # find A in original edge-dict
#             if pair[1] in edge_dict[pair[0]] and pair[0] in edge_dict[pair[1]]:
#                 A = 1
#             else:
#                 A = 0
# #             print("I ma 222222222222222222222222222222222222222222222222222222",A)
#             mod_s += float(A-ki*kj/(2*m))
#     res = float(mod_s/(2*m))
#     return community, res
                
def compute_mod(community, n_edge_dict, edge_dict, m,A_matrix):
    mod_s = 0
#     A = 0
    for one_comm in community:
#         all_pair = itertools.combinations(list(one_comm),2)
        all_pair = itertools.combinations(list(one_comm),2)
        for pair in all_pair:
            ki = len(n_edge_dict[pair[0]])
            kj = len(n_edge_dict[pair[1]])
            # find A in original edge-dict
#             temp_key = tuple(sorted([pair[0],pair[1]]))
#             A = 1 if temp_key in A_matrix else 0
#             print(A)
            if pair[1] in edge_dict[pair[0]] and pair[0] in edge_dict[pair[1]]:
                A = 1
            else:
                A = 0
#             print("I ma 222222222222222222222222222222222222222222222222222222",A)
            mod_s += float(A-(ki*kj/(2*m)))
    res = float(mod_s/(2*m))
    return community, res

def opt_comm(vertex,n_edge_dict, edge_dict, btw_list, m):
    opt_comm = None
    max_mod = float("-inf")
#     i = 0
    while len(btw_list) >0:
        new_edge_dict = cut_high_edge(n_edge_dict, btw_list)
        communities = dectect_num_comm(vertex, new_edge_dict)
        community, mod = compute_mod(communities, edge_dict,edge_dict, m, A_matrix)
        btw_list = combtw(vertex, new_edge_dict)
        n_edge_dict = new_edge_dict
        if mod > max_mod:
            max_mod = mod
            opt_comm = community
#         print(max_mod, len(opt_comm))

        
    return sorted(opt_comm, key=lambda item: (len(item), item[0]))
#         return opt_comm 
community_list = opt_comm(vertex, edge_dict,edge_dict, btw_list, m)
print('len of comm', len(community_list))
print(community_list)

filename = out_path_btw
with open(filename,'w') as zaili:
    for a in btw_list:
        zaili.write(str(a[0])+', '+ str(a[1])+'\n')
        
filename = out_path_comm       
with open(filename,'w') as zailip:
    for i in range(len(community_list)):
        a = community_list[i]
        l = len(a)
        for i in range(l):
            if i == l-1:
                zailip.write("'"+str(a[i])+ "'")
            else:
                zailip.write("'"+str(a[i])+ "'"+', ')
        zailip.write("\n")
#     b = community_list[len(community_list)-1]
#     kk = len(b)
#     for i in range(kk):
#         if i == kk-1:
#             zailip.write(str(b[i]))
#         else:
#             zailip.write(str(b[i])+', ')    
end = time.time()
print("Duration", end-start)




