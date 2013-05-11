
var neo4j = require('node-neo4j'),
    redis = require('redis'),
    uuid = require('node-uuid'),
    neo4j2 = require('neo4j');


/**
 * Init Zoot.
 */
var Zoot = function (neoUrl, redisHost, redisPort)
{
    this.neoClient = new neo4j(neoUrl);
    this.neoClient2 = new neo4j2.GraphDatabase(neoUrl);
    this.redisClient = redis.createClient(redisPort, redisHost);
};


/**
 * Add a task.
 */
Zoot.prototype.addTask = function (taskData, searchTags, addTags, removeTags, markDirty)
{
    if (searchTags == null) //no parent
    {
        key = uuid.v1();
        this.redisClient.set("task:" + key, JSON.stringify(taskData), redis.print);
        this.neoClient.insertNode({dirty:markDirty, tag:addTags.sort(), key:key, query:""}, function (err, node) {
            if (err) throw err;
            console.log(node.id);        
        });
    }
    else //search for matching nodes
    {
        var self = this;
        var tags = [];
        
        for (var tag in searchTags.sort())
        {
            tags.push('tag:*' + tag + '* ');
        }

        var query = "start nodes=node:node_auto_index('" + tags.join(" AND ") + "') return nodes;";

        self.neoClient.cypherQuery(query, function(err, result) {
            if(err) throw err;

            for (index in result.data)
            {
                var task = result.data[index];
                var taskNode = result.data[index].data;
                var taskInfo = { task: task, taskNode: taskNode };
                var newTags = self._combineTags(taskNode.tag, addTags, removeTags);
                var findExistingChildren = "start a=node(" + task.id + ") match a<-[DEPENDS_ON]-b return b;";

                self.redisClient.get("task:" + taskNode.key, function (err, taskData) {
                    if (err) throw err;

                    self.neoClient.cypherQuery(findExistingChildren, function(err, existingChildren) {
                        if(err) throw err;

                        var nodeExists = false;
                        for (existingIndex in existingChildren.data) 
                        {
                            var existingChild = existingChildren.data[ecIndex];
                            if (existingChild.data.query == query && existingChild.data.tag == newTags)
                            {
                                existingChild.data.dirty = String(dirty);
                                self.neoClient.updateNode(existingChild.id, existingChild.data, function(err, node) {
                                    if(err) throw err;
                                });
                                nodeExists = true;
                                break;
                            }
                        }

                        if (!nodeExists)
                        {
                            key = uuid.v1();
                            self.redisClient.set("task:" + key, JSON.stringify(taskData), redis.print);
                            self.neoClient.insertNode({dirty: dirty, tag: newTags, key: key, query: query }, function (err, newNode) {
                                if (err) throw err;
                                console.log("" + this.task.id + "-->" + newNode.id);
                                self.neoClient.insertRelationship(newNode.id, this.task.id, 'DEPENDS_ON', {}, function (err, relation) {
                                    if (err) throw err;
                                });
                            }.bind(this));
                        }
                    }.bind(this));
                }.bind(taskInfo));
            }
        });
    }    

};


/**
 * Get a dirty task.
 */
Zoot.prototype.getTask = function (searchTags, callback)
{
    var self = this;
    var tags = [];

    for (var v in searchInfo.tagsMatchAll.sort())
    {
        tags.push('tag:*' + searchTags + '* ');
    }

    var findDirtyRoot = "start a=node:node_auto_index('dirty: true AND " + tags.join(" AND ") + "') match a-[r?:DEPENDS_ON]->b WHERE r is null return a;";
    
    self.neoClient.cypherQuery(findDirtyRoot, function(err, result) {

        if (err) throw err;

        var tryLock = function (index, nodes, callback, exitOnNone, childParent) {

            if (index < nodes.length)
            {
                var thisNode = {};
                if (childParent)
                {
                    thisNode.parent = nodes[index]["parent"]._data.data;
                    thisNode.child = nodes[index]["child"]._data.data;
                    thisNode.childId = nodes[index]["childId"];
                }
                else
                {
                    thisNode.parent = null;
                    thisNode.child = nodes[index].data;
                    thisNode.childId = nodes[index].id;
                }

                self.redisClient.setnx("queue:" + thisNode.child.key , thisNode, function(err, reply) {

                    if (reply == 1)
                    {
                        self.redisClient.get("task:" + thisNode.child.key, function (err, data) {
                            if (err) throw err;
                            callback(thisNode, JSON.parse(data));
                        });
                    }
                    else
                    {
                        tryLock(index+1, nodes, callback, false, childParent);
                    }
                });
            }
            else
            {
                if (exitOnNone)
                {
                    console.log("No tasks found.");
                    callback(null, null);
                }
                else
                {
                    var findDirtyOneLevel = "start a=node:node_auto_index('dirty: true AND " + tags.join(" AND ") +
                        "'), b=node:node_auto_index('dirty: false') match a-[DEPENDS_ON*1..1]->b return a as child, ID(a) as childId, b as parent;";

                    self.neoClient2.query(findDirtyOneLevel, {}, function(err, result) {   
                        if (result)
                        {           
                            tryLock(0, result, callback, true, true);
                        }
                    });
                }
            }
            
        };

        tryLock(0, result.data, callback, false, false);
    });
};


/**
 * Release a locked task.
 */
Zoot.prototype.releaseTask = function (task, complete, callback)
{
    var self = this;
    
    if (complete)
    {
        task.child.dirty = 'false';
        self.neoClient.updateNode(task.childId, task.child, function(err, node) {
            if(err) throw err;
            self.redisClient.del("queue:" + task.child.key, function(err, val) {
                if(err) throw err;
                callback();
            });
        });
    }
    else
    {
        self.redisClient.del("queue:" + taskInfo.child.key, function(err, val) {
            if (err) throw err;
            callback();
        });
    }
};


/**
 * Create a new tag list.
 */
Zoot.prototype._combineTags = function (existingTags, addTags, removeTags)
{
    var tags = [];

    for (tag in existingTags)
    {
        if (removeTags.indexOf(tag) < 0)
        {
            tags.push(tag);
        }
    }

    tags.push.apply(addTags);

    return tags.sort();
};


/**
* Export classes and modules.
*/
module.exports = Zoot;


