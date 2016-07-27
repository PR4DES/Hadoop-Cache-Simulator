#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <string>
#include <vector>
#include <queue>
using namespace std;

struct CompareApps;
class NameNode;
class File;
class Data;
class Node;
class Container;
class ResourceManager;
class Application;

int main_time = 0;

// Data class : Block of file, declared in File class
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
		int GetFileIdx();
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
		Data **datas;
	public:
		File(int fileidx, int filesize, int nodenum);
		int GetFileIdx();
		int GetFileSize();
		Data GetData(int dataidx, int order); // maximum 3 order (becasue # of replica is 3)
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
	public:
		Node(int nodeidx, int cachesize, int containernum);
};

// Container class : declared in Node class
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
};

// Application class
class Application {
	friend class ResourceManager;
	friend struct CompareApps;
	private:
		int app_idx;
		string app_name;
		int file_idx;		// index of file needed for this application
		int mapper_num;	// == # of blocks
		int reducer_num;
		int skip_count;
		int skip_threshold;
		int cache_local_avg_map_time;
		int data_local_avg_map_time;
		int rack_local_avg_map_time;
		int avg_reduce_time;
	public:
		Application(int appidx, string appname, int fileidx, int mapnum, int reducenum, int skipcount, int skipthreshold,\
		int cachetime, int datatime, int racktime, int reducetime);
};

struct CompareApps {
	bool operator()(Application const& a1, Application const& a2)  {
		return a1.mapper_num > a2.mapper_num;
	}
};

// Resource Manager class
class ResourceManager {
	private:
		priority_queue<Application, vector<Application>, CompareApps> job_queue;
	public:
		void DelayScheduling();
		void AddJob(Application app);
};

// Data class implementation
Data::Data() {
	is_cached = false;
}
int Data::GetFileIdx() { return file_idx; }
int Data::GetDataIdx() { return data_idx; }
int Data::GetNodePosition() { return node_position; }
bool Data::IsCached() { return is_cached; }

// File class implementation
File::File(int fileidx, int filesize, int nodenum) {
	file_idx = fileidx;
	file_size = filesize;
	datas = new Data*[file_size];
	int replica = 3;
	for(int i=0; i<file_size; i++) {
		datas[i] = new Data[replica];
		for(int j=0; j<replica; j++) {
			datas[i][j].file_idx = file_idx;
			datas[i][j].data_idx = i;
			datas[i][j].node_position = rand()%nodenum;
		}
	}
}

int File::GetFileIdx() { return file_idx; }
int File::GetFileSize() { return file_size; }
Data File::GetData(int dataidx, int order) { return datas[dataidx][order]; }

// NameNode class implementation
NameNode::NameNode(int nodenum) {
	node_num = nodenum;
	file_num = 0;
	node_fail_check = new bool[node_num];
	for(int i=0; i<node_num; i++) {
		node_fail_check = false;
	}
}
void NameNode::AddFile(File* f) {
	files.resize(files.size());
	files.push_back(f);
	file_num++;
}
int NameNode::GetFileNum() { return file_num; }

// Node class implementation
Node::Node(int nodeidx, int cachesize, int containernum) {
	node_idx = nodeidx;
	is_fail = false;
	cache_size = cachesize;
	container_num = containernum;
	containers = new Container[container_num];
}

// Container class implementation
Container::Container() {
	is_working = false;
}
// Application class implementation
Application::Application(int appidx, string appname, int fileidx, int mapnum, int reducenum, int skipcount, int skipthreshold,\
		int cachetime, int datatime, int racktime, int reducetime) {
	app_idx = appidx;
	app_name = appname;
	file_idx = fileidx;
	mapper_num = mapnum;
	reducer_num = reducenum;
	skip_count = skipcount;
	skip_threshold = skipthreshold;
	cache_local_avg_map_time = cachetime;
	data_local_avg_map_time = datatime;
	rack_local_avg_map_time = racktime;
	avg_reduce_time = reducetime;
}

// Resource Manager class implementation
void ResourceManager::DelayScheduling() {

}
void ResourceManager::AddJob(Application app) {
	job_queue.push(app);
}

// Here is main :)
int main() {
	srand((unsigned int)time(NULL));

	// 1. Node setting
	int node_num, cache_size, container_num;
	cout << "Set the number of node, size of cache, and number of container.\n";
	cin >> node_num >> cache_size >> container_num;

	Node* nodes[node_num];
	for(int i=0; i<node_num; i++) {
		nodes[i] = new Node(i, cache_size, container_num);
	}

	NameNode namenode(node_num);
	// 2. File setting
	int file_size;
	int i=0;
	cout << "Set the size of each file, if it is done then put -1\n";
	while(1) {
		cin >> file_size;
		if(file_size == -1) break;
		
		File* f = new File(i, file_size, node_num);
		namenode.AddFile(f);

		i++;
	}

	// 3. Job setting
	ResourceManager();
	string app_name;
	int app_idx, file_idx, mapper_num, reducer_num, skip_count, skip_threshold, cache_time, data_time, rack_time, reduce_time;

	return 0;
}
