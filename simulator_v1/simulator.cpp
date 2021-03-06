#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <string>
#include <vector>
#include <queue>
using namespace std;

struct CompareApps;
struct CompareSubmitTime;
class NameNode;
class File;
class Data;
class Node;
class Container;
class ResourceManager;
class Application;
class CacheReplacement;

int main_time = 0;

// Data class
class Data {
	friend class File;
	friend class NameNode;
	private:
		int file_idx;			// index of file where this data located
		int data_idx;			// index of this data in the file
		int node_position;	// index of node where this data located
		bool is_cached;
	public:
		Data();
		int GetDataIdx();
		int GetNodePosition();
		bool IsCached();
};

// File class
class File {
	friend class NameNode;
	private:
		int file_idx;			// index of file
		int file_size;			// number of data blocks
		int replica_num;
		Data **datas;
	public:
		File(int fileidx, int filesize, int nodenum, int replicanum);
		int GetFileIdx();
		int GetFileSize();
		int GetReplicaNum();
		Data GetData(int dataidx, int order); // order of replicas
};

// NameNode class
class NameNode {
	private:
		int node_num;			// number of nodes
		int file_num;			// number of file
		bool *node_fail_check;	// (#=node_num)boolean array for checking node fail
	public:
		vector<File*> files;
		NameNode(int nodenum);
		void AddFile(File *f);
		int GetFileNum();
		int GetNodeNum();
};

// Node class
class Node {
	friend class NameNode;
	friend class ResourceManager;
	private:
		int node_idx;
		bool is_fail;
		int cache_size;
		int container_num;
		Container *containers;
		CacheReplacement *cache_replace;
	public:
		Node(int nodeidx, int cachesize, int containernum);
		Container GetContainer(int containeridx);
		int GetContSize();
};

// Container class
class Container {
	friend class Node;
	friend class NameNode;
	friend class ResourceManager;
	private:
		int start_time;
		int end_time;
		Application *task;
		int task_idx;
		bool is_working;
	public:
		Container();
		bool GetIsWorking();
		void StateChange();
		void TaskRun(Application *job, int tasknum, int run_time);
};

// Application class
class Application {
	friend class ResourceManager;
	friend struct CompareApps;
	friend struct CompareSubmitTime;
	friend class Container;
	private:
		int app_idx;
		string app_name;
		int file_idx;		// index of file needed for this application
		int mapper_num;	// == # of blocks == # of tasks
		int reducer_num;
		int skip_count;
		int skip_threshold;
		int avg_cache_time;
		int avg_data_time;
		int avg_rack_time;
		int avg_reduce_time;
		int occupied_container;
		// when all boolean are true and completed task number is same as mapper number then this job is done.
		bool *is_task_working;	// when each task start, it become true
		int working_map_num;
		int working_reduce_num;
		int completed_task_num;
		int submit_time;
		int flag;
	public:
		Application(int appidx, string appname, int fileidx, int reducenum, int skipthreshold,\
		int cachetime, int datatime, int racktime, int reducetime, NameNode namenode, int submittime);
};

struct CompareApps {
	bool operator()(Application const *a1, Application const *a2)  {
		return a1->occupied_container > a2->occupied_container;
	}
};
struct CompareSubmitTime {
	bool operator()(Application const *a1, Application const *a2) {
		return a1->submit_time > a2->submit_time;
	}
};

// Resource Manager class
class ResourceManager {
	private:
		priority_queue<Application*, vector<Application*>, CompareApps> job_queue;
		priority_queue<Application*, vector<Application*>, CompareSubmitTime> job_submit_queue;
	public:
		void DelayScheduling(NameNode namenode, Node* node, int containernum);
		void JobCompleteManager(NameNode namenode, Node* nodes[]);
		void AddJob(Application *app);
		void AddSubmitJob(Application *app);
		bool IsQueueEmpty();
		void Show();		// for test print
		bool IsReducePhase();
		void ReduceTask(NameNode namenode, Node* node, int countainernum);
		void JobSubmit();
};

// Cache Replacement class
class CacheReplacement {
	friend class NameNode;
	private:
		Data** cached_datas;
		int victim;
		int cached_num;		// number of cached data block
	public:
		CacheReplacement(int cachesize);
		virtual void Add() = 0;
		virtual void SelectVictim() = 0;
};

class Random : public CacheReplacement {
	public:
		Random(int cachesize);
		virtual void Add();
		virtual void SelectVictim();
};

// Data class implementation
Data::Data() { 
	is_cached = false; 
}
int Data::GetDataIdx() { return data_idx; }
int Data::GetNodePosition() { return node_position; }
bool Data::IsCached() { return is_cached; }

// File class implementation
File::File(int fileidx, int filesize, int nodenum, int replicanum) {
	file_idx = fileidx;
	file_size = filesize;
	replica_num = replicanum;
	datas = new Data*[file_size];
	for(int i=0; i<file_size; i++) {
		datas[i] = new Data[replica_num];
		for(int j=0; j<replica_num; j++) {
			datas[i][j].file_idx = file_idx;
			datas[i][j].data_idx = i;
			int temp;
			bool check = 1;
			while(check) {
				temp = rand()%nodenum;
				check = 0;
				for(int k=0; k<j; k++) {
					if(temp == datas[i][k].node_position) {
						check = 1;
						break;
					}
				}
				datas[i][j].node_position = temp;
			}
		}
	}
}
int File::GetFileIdx() { return file_idx; }
int File::GetFileSize() { return file_size; }
int File::GetReplicaNum() { return replica_num; }
Data File::GetData(int dataidx, int order) { return datas[dataidx][order]; }

// NameNode class implementation
NameNode::NameNode(int nodenum) {
	node_num = nodenum;
	file_num = 0;
	node_fail_check = new bool[node_num];
	for(int i=0; i<node_num; i++) {
		node_fail_check[i] = false;
	}
}
void NameNode::AddFile(File* f) {
	files.resize(files.size());
	files.push_back(f);
	file_num++;
}
int NameNode::GetFileNum() { return file_num; }
int NameNode::GetNodeNum() { return node_num; }

// Node class implementation
Node::Node(int nodeidx, int cachesize, int containernum) {
	node_idx = nodeidx;
	is_fail = false;
	cache_size = cachesize;
	container_num = containernum;
	containers = new Container[container_num];
	cache_replace = NULL;
}
Container Node::GetContainer(int containeridx) { return containers[containeridx]; }
int Node::GetContSize() { return container_num; }

// Container class implementation
Container::Container() {
	is_working = false;
}
bool Container::GetIsWorking() { return is_working; }
void Container::StateChange() { is_working = !is_working; }
void Container::TaskRun(Application *job, int tasknum, int run_time) {
	task = job;
	task_idx = tasknum;
	start_time = main_time;
	end_time = start_time + run_time;
	is_working = true;
	if(task_idx != -1) {
		task->is_task_working[task_idx] = true;
	}
}

// Application class implementation
Application::Application(int appidx, string appname, int fileidx, int reducenum, int skipthreshold,\
		int cachetime, int datatime, int racktime, int reducetime, NameNode namenode, int submittime) {
	app_idx = appidx;
	app_name = appname;
	file_idx = fileidx;
	mapper_num = namenode.files[file_idx]->GetFileSize();
	reducer_num = reducenum;
	skip_count = 0;
	skip_threshold = skipthreshold;
	avg_cache_time = cachetime;
	avg_data_time = datatime;
	avg_rack_time = racktime;
	avg_reduce_time = reducetime;
	occupied_container = 0;
	is_task_working = new bool[mapper_num];
	for(int i=0; i<mapper_num; i++) {
		is_task_working[i] = false;
	}
	working_map_num = 0;
	working_reduce_num = 0;
	completed_task_num = 0;
	submit_time = submittime;
	flag = 0;
}

// Resource Manager class implementation
void ResourceManager::DelayScheduling(NameNode namenode, Node* node, int containernum) {
	if(job_queue.empty()) return;
	if(IsReducePhase()) {
		ReduceTask(namenode, node, containernum);
		return;
	}
	if(job_queue.top()->completed_task_num >= job_queue.top()->mapper_num) return;
	bool checkFlag = true;
	for(int i=job_queue.top()->flag; i<job_queue.top()->mapper_num; i++) {		// job_queue.top()->mapper_num == file block size
		if(!job_queue.top()->is_task_working[i]) {
			for(int j=0; j<namenode.files[job_queue.top()->file_idx]->GetReplicaNum(); j++) {
				if(node->node_idx == namenode.files[job_queue.top()->file_idx]->GetData(i, j).GetNodePosition()) {
					if(namenode.files[job_queue.top()->file_idx]->GetData(i,j).IsCached()) {
						node->containers[containernum].TaskRun(job_queue.top(), i, job_queue.top()->avg_cache_time);
					} else {
						node->containers[containernum].TaskRun(job_queue.top(), i, job_queue.top()->avg_data_time);
					}
					job_queue.top()->occupied_container++;
					job_queue.top()->skip_count = 0;
					job_queue.top()->working_map_num++;
					if(checkFlag) job_queue.top()->flag = i+1;
					// resort job queue
					job_queue.push(job_queue.top());
					job_queue.pop();
					return;
				} else if(j == namenode.files[job_queue.top()->file_idx]->GetReplicaNum()-1 && job_queue.top()->skip_count >= job_queue.top()->skip_threshold) {
					//Over the skip threshold
					node->containers[containernum].TaskRun(job_queue.top(), i, job_queue.top()->avg_rack_time);
					job_queue.top()->occupied_container++;
					job_queue.top()->skip_count = 0;
					job_queue.top()->working_map_num++;
					if(checkFlag) job_queue.top()->flag = i+1;
					job_queue.push(job_queue.top());
					job_queue.pop();
					return;
				}
			}
			checkFlag = false;
		}
	}
	// Delay scheduling
	job_queue.top()->skip_count++;
	Application* temp = job_queue.top();
	job_queue.pop();
	DelayScheduling(namenode, node, containernum);
	job_queue.push(temp);
}
void ResourceManager::JobCompleteManager(NameNode namenode, Node* nodes[]) {
	for(int i=0; i<namenode.GetNodeNum(); i++) {
		for(int j=0; j<nodes[i]->container_num; j++) {
			if(nodes[i]->containers[j].GetIsWorking() && nodes[i]->containers[j].end_time <= main_time) {
				nodes[i]->containers[j].task->completed_task_num++;
				nodes[i]->containers[j].is_working = false;
				nodes[i]->containers[j].task->occupied_container--;
			}
		}
	}

	// check whether job is done or not and reorder
	priority_queue<Application*, vector<Application*>, CompareApps> new_job_queue;
	while(!job_queue.empty()) {
		new_job_queue.push(job_queue.top());
		job_queue.pop();
	}
	while(!new_job_queue.empty()) {
		if(new_job_queue.top()->completed_task_num >= new_job_queue.top()->mapper_num + new_job_queue.top()->reducer_num) {
			cout << "Job " << new_job_queue.top()->app_name << " is done in " << main_time << " second!\n";
			new_job_queue.pop();
		} else {
			job_queue.push(new_job_queue.top());
			new_job_queue.pop();
		}
	}
}

void ResourceManager::AddJob(Application *app) {
	job_queue.push(app);
}
void ResourceManager::AddSubmitJob(Application *app) {
	job_submit_queue.push(app);
}
bool ResourceManager::IsQueueEmpty() { return (job_queue.empty() && job_submit_queue.empty()); }
bool ResourceManager::IsReducePhase() {
	if(job_queue.top()->completed_task_num >= job_queue.top()->mapper_num && \
	job_queue.top()->completed_task_num+job_queue.top()->working_reduce_num < job_queue.top()->mapper_num + job_queue.top()->reducer_num) 
		return true;
	return false;
}
void ResourceManager::ReduceTask(NameNode namenode, Node* node, int containernum) {
	if(job_queue.top()->avg_reduce_time == 0) {
		cout << "Job " << job_queue.top()->app_name << " is done in " << main_time << " second!\n";
		job_queue.pop();
		return;
	}
	node->containers[containernum].TaskRun(job_queue.top(), -1, job_queue.top()->avg_reduce_time);
	job_queue.top()->occupied_container++;
	job_queue.top()->working_reduce_num++;
	job_queue.push(job_queue.top());
	job_queue.pop();
	return;
}
void ResourceManager::JobSubmit() {
	while(!job_submit_queue.empty()) {
		if(job_submit_queue.top()->submit_time <= main_time) {
			job_queue.push(job_submit_queue.top());
			job_submit_queue.pop();
		} else {
			break;
		}
	}
}
// for test print
void ResourceManager::Show() {
	for(int i=0; i<job_queue.top()->mapper_num; i++) {
		cout << job_queue.top()->is_task_working[i];
	}
	cout << "    " << job_queue.top()->app_name << endl;
}

CacheReplacement::CacheReplacement(int cachesize) {
	cached_datas = new Data*[cachesize];
	victim = -1;
	cached_num = 0;
}

// Chche replacement implementation
Random::Random(int cachesize) : CacheReplacement(cachesize) {
}
void Random::Add() {
}
void Random::SelectVictim() {
}

// Here is main :)
int main() {
	srand((unsigned int)time(NULL));

	// 1. Node setting
	int node_num, cache_size, container_num, replica_num;
	cout << "Set the number of node, size of cache, number of container, and replica number.\n";
	cin >> node_num >> cache_size >> container_num >> replica_num;
	if(replica_num > node_num) {
		cout << "wrong input : replica is more than number of node.\n";
		return 1;
	}

	Node* nodes[node_num];
	for(int i=0; i<node_num; i++) {
		nodes[i] = new Node(i, cache_size, container_num);
	}

	NameNode namenode(node_num);
	// 2. File setting : distribute data blocks
	int file_size;
	int file_numbering=0;
	cout << "Set the size of each file, if it is done then put -1\n";
	while(1) {
		cin >> file_size;
		if(file_size == -1) break;

		File* f = new File(file_numbering, file_size, node_num, replica_num);
		namenode.AddFile(f);

		file_numbering++;
	}

	// 3. Job setting
	ResourceManager resourcemanage;
	int app_idx = 0;
	string app_name;
	int file_idx, reduce_num, skip_threshold, cache_time, data_time, rack_time, reduce_time, submit_time;
	cout << "Put inputs in this order: index of file, name of job, number of reducer, skip threshold, average time for cache local, data local, rack local, reduce and submit time.\n";
	cout << "If it is done then put -1\n";
	while(1) {
		cin >> file_idx;
		if(file_idx == -1) break;
		cin >> app_name >> reduce_num >> skip_threshold >> cache_time >> data_time >> rack_time >> reduce_time >> submit_time;

		if(file_idx >= namenode.GetFileNum()) {
			cout << "wrong input : file index is larger than number of existing file!\n";
			continue;
		}
		if(reduce_time == 0) reduce_num = 0;
		Application* app = new Application(app_idx, app_name, file_idx, reduce_num, skip_threshold, cache_time, data_time, rack_time, reduce_time, namenode, submit_time);
		resourcemanage.AddSubmitJob(app);

		app_idx++;
	}

	cout << "Main task start" << endl;
	// 4. Main Task Start
	while(1) {
		resourcemanage.JobSubmit();
		resourcemanage.JobCompleteManager(namenode, nodes);
		if(resourcemanage.IsQueueEmpty()) break;

		for(int i=0; i<node_num; i++) {
			for(int j=0; j<container_num; j++) {
				if(!nodes[i]->GetContainer(j).GetIsWorking()) {
					resourcemanage.DelayScheduling(namenode, nodes[i], j);
				}
			}
		}
		main_time++;
	}
	cout << "Final main time is " << main_time << " seconds.\n";

	return 0;
}
