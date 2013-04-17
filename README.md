zoot
====

A structured task queue in Zookeeper
--------------------------------

Zoot is designed to make it easy to queue dependent tasks utilising [Apache Zookeeper](http://http://zookeeper.apache.org/).

Zoot is used to manage large dependent task chains where changes to any link level require a refresh job to be queued for all nodes further down the chain.

Create A Zoot Client
--------------------

    var Zoot = require("zoot");
    var zootClient = new Zoot("my-zk-server", 2181, "myworld", function () { /* on connect */ });

Initialising a client ensures a parent node "myworld" and the initial nodes:

* pending - pending tasks
* working - working tasks
* complete - complete tasks
* tasks - task chain root

are created on the target Zookeeper instance.

Build Task Chain
----------------

    zootClient.add(taskInfo);

### taskInfo
A JSON structure with:

* id - a UUID to identify this task.
* parent - an ordered array of UUID which represent the path to this task.
* tags - user-defined identification.
* data - optional JSON task data.

    zootClient.addSearch(searchInfo, transform(parent, task) { return task; });

### searchInfo
A JSON structure with:

* matchAll - an array of parent UUIDs to match. Created task with have multiple parents.
* matchAny - an array of parent UUIDs to match. Will potentially create multiple tasks with single parent.
* exclude - an array of parent UUIDs to reject.
* tagsMatchAll - an array of parent tags to match. Will potentially create multiple tasks with single parent.
* tagsExclude - an array of parent tags to reject.

### transform
A function with the prototype:

    taskInfo function (taskInfo parent, taskInfo task);

This function allows the client to customize the created child task.

Defining Tasks
--------------

The definition of UUIDs (and how unique they are) is entirely up to the client.



