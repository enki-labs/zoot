
var neo4j = require('node-neo4j'),
    redis = require('redis'),
    uuid = require('node-uuid'),
    neo4j2 = require('neo4j'),
    RefCounter = require('./refCounter.js');


/**
 * Init Zoot.
 */
var Zoot = function (neoUrl, redisHost, redisPort, readyCallback)
{
    this.neoClient = new neo4j(neoUrl);
    this.neoClient2 = new neo4j2.GraphDatabase(neoUrl);
    this.redisClient = redis.createClient(redisPort, redisHost);
    this.redisClient.on("ready", function (err) {
        if (err) throw err;
        readyCallback();
    });
};


/**
 * Clear the entire task tree.
 */
Zoot.prototype.yes_i_really_want_to_delete_everything = function (callback)
{
    var self = this;

    self.redisClient.flushdb(function () {
        self.neoClient.cypherQuery("start r=relationship(*) delete r;", function () {
            self.neoClient.cypherQuery("start n=node(*) delete n;", function () {
                callback();
            });
        });
    });
};


/**
 * Add a task.
 */
Zoot.prototype.addTask = function (taskData, searchTags, addTags, removeTags, markDirty, callback)
{
    var self = this;
    var refCounter = new RefCounter(callback);

    if (searchTags == null) //no parent
    {
        var tags = [];        
        for (var tagIndex in addTags.sort())
        {
            tags.push('tag:*' + addTags[tagIndex] + '* ');
        }

        var query = "start nodes=node:node_auto_index('" + tags.join(" AND ") + "') return nodes;";
        self.neoClient.cypherQuery(query, function(err, existingNodes) {
            if (existingNodes != null && existingNodes.data.length > 0)
            {
                var existingNode = existingNodes.data[0];
                existingNode.data.dirty = String(markDirty);
                self.neoClient.updateNode(existingNode.id, existingNode.data, function(err, node) {
                    if(err) throw err;
                    if (markDirty) { self._markDirty(existingNode, refCounter); }
                    else { callback(); }
                });
            }
            else
            {
                key = uuid.v1();
                this.redisClient.set("task:" + key, JSON.stringify(taskData), function (err) { 
                    if (err) throw err;

                    //NB!! Query must be space rather than empty string or Neo4j errors silently.
                    var nodeInfo = {'dirty':markDirty, 'tag':addTags.sort(), 'key':key, 'query':' '};
                    this.neoClient.insertNode(nodeInfo, function (err, node) {
                        if (err) throw err;
                        callback();
                    }.bind(this));

                }.bind(this));
            }
        }.bind(this));
    }
    else //search for matching nodes
    {
        var tags = [];
        
        for (var tagIndex in searchTags.sort())
        {
            tags.push('tag:*' + searchTags[tagIndex] + '* ');
        }

        var query = "start nodes=node:node_auto_index('" + tags.join(" AND ") + "') return nodes;";
        
        self.neoClient.cypherQuery(query, function(err, result) {
            if(err) throw err;

            for (index in result.data)
            {
                refCounter.start();
                this.task = result.data[index];
                this.taskNode = result.data[index].data;
                this.newTags = self._combineTags(taskNode.tag, addTags, removeTags);
                this.findExistingChildren = "start a=node(" + task.id + ") match a<-[DEPENDS_ON]-b return b;";

                self.redisClient.get("task:" + taskNode.key, function (err, parentData) {
                    if (err) throw err;

                    self.neoClient.cypherQuery(this.findExistingChildren, function(err, existingChildren) {
                        if(err) throw err;

                        var nodeExists = false;
                        for (existingIndex in existingChildren.data) 
                        {
                            var existingChild = existingChildren.data[existingIndex];
                            if (existingChild.data.query == query && existingChild.data.tag.toString() == this.newTags.toString())
                            {
                                existingChild.data.dirty = String(markDirty);
                                if (markDirty) { self._markDirty(existingChild, refCounter); }
                                self.neoClient.updateNode(existingChild.id, existingChild.data, function(err, node) {
                                    if(err) throw err;
                                    refCounter.end();
                                });
                                nodeExists = true;
                                break;
                            }
                        }

                        if (!nodeExists)
                        {
                            key = uuid.v1();
                            self.redisClient.set("task:" + key, JSON.stringify(taskData), function (err, data) {

                                if (err) throw err;

                                self.neoClient.insertNode({dirty: markDirty, tag: this.newTags, key: key, query: query }, function (err, newNode) {
                                
                                    if (err) throw err;
                                    this.newTaskNode = newNode;
                                    self.neoClient.insertRelationship(newNode.id, this.task.id, 'DEPENDS_ON', {}, function (err, relation) {
                                        if (err) throw err;
                                        if (markDirty) { self._markDirty(this.newTaskNode, refCounter); }
                                        refCounter.end();
                                    }.bind(this));
                                }.bind(this));
                            }.bind(this));
                        }
                    }.bind(this));
                }.bind(this));
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

    for (var tagIndex in searchTags.sort())
    {
        tags.push('tag:*' + searchTags[tagIndex] + '* ');
    }

    var findDirtyRoot = "start a=node:node_auto_index('dirty: true AND " + tags.join(" AND ") + "') match a-[r?:DEPENDS_ON]->b WHERE r is null return a as child;";
    
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

                            thisNode.childData = JSON.parse(data);

                            if (thisNode.parent == null)
                            {
                                callback(thisNode);
                            }
                            else
                            {
                                self.redisClient.get("task:" + thisNode.parent.key, function (err, data) {
                                    if (err) throw err;
                                    thisNode.parentData = JSON.parse(data);
                                    callback(thisNode);
                                });
                            }
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
                    callback(null);
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
    if (task == null) throw { name: "Argument Exception", message: "Attempted to release null task" };
    
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
        self.redisClient.del("queue:" + task.child.key, function(err, val) {
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

    for (tagIndex in existingTags)
    {
        var tag = existingTags[tagIndex];
        if (removeTags.indexOf(tag) < 0)
        {
            tags.push(tag);
        }
    }
    
    for (tagIndex in addTags)
    {
        tags.push(addTags[tagIndex]);
    }

    return tags.sort();
};


/**
 * Mark children dirty.
 */
Zoot.prototype._markDirty = function (parentNode, refCounter)
{
    var self = this;
    var query = "start a=node(" + parentNode.id + ") match a<-[DEPENDS_ON*1..1000]-b return b;";
    refCounter.start();

    self.neoClient.cypherQuery(query, function(err, children) {
        for (childIndex in children.data)
        {
            var child = children.data[childIndex];
            refCounter.start();
            child.data.dirty = 'true';
            self.neoClient.updateNode(child.id, child.data, function(err, result) {
                if(err) throw err;
                refCounter.end();
            });
        }
        refCounter.end();
    });
};


/**
* Export classes and modules.
*/
module.exports = Zoot;

