
var Assert = require('assert')
, Zoot = require('./../lib/main')
, Step = require('step')
, Util = require('util');

var testParams = {
    neoUrl:     'http://omnius01:7474'
,   redisHost:  'omnius01'
,   redisPort:  6379
};

suite('zoott queue tests', function () {

    var that = this;
    setup( function(done) { that.zc = new Zoot(testParams.neoUrl, testParams.redisHost, testParams.redisPort, done); });

    test('create a task', function (done)
    {
        var searchTags = null;
        var addTags = ['test', 'level1'];
        var removeTags = [];
        var taskData = {'abc':'123'};
        var markDirty = true;

        Assert.doesNotThrow ( function () {

            Step(
                    function () { that.zc.yes_i_really_want_to_delete_everything(this); },
                    function () { that.zc.addTask(taskData, searchTags, addTags, removeTags, markDirty, this); },
                    function () { that.zc.getTask(addTags, this); },
                    function (taskInfo) { Assert.notEqual(taskInfo, null, "task not found"); that.lockedTask = taskInfo; that.zc.getTask(addTags, this); },
                    function (taskInfo) { Assert.equal(taskInfo, null, "task locked twice"); that.zc.releaseTask(that.lockedTask, false, this); },
                    function () { that.zc.getTask(addTags, this); },
                    function (taskInfo) { Assert.notEqual(taskInfo, null, "task was not released"); done(); }
                );
        });
    });

    test('create a child task', function (done)
    {
        var parentSearchTags = null;
        var parentAddTags = ['test', 'level2-parent'];
        var parentRemoveTags = [];
        var parentData = {'parent':'data'};
        
        var childSearchTags = parentAddTags;
        var childAddTags = ['level2-child'];
        var childRemoveTags = ['level2-parent'];
        var childData = {'child':'data'};

        Assert.doesNotThrow ( function () {

            Step(
                    function () { that.zc.yes_i_really_want_to_delete_everything(this); },
                    function () { that.zc.addTask(parentData, parentSearchTags, parentAddTags, parentRemoveTags, true, this); },
                    function () { that.zc.addTask(childData, childSearchTags, childAddTags, childRemoveTags, true, this); },
                    function () { that.zc.getTask(['test'], this); },
                    function (taskInfo) {   Assert.notEqual(taskInfo, null, "parent task not found"); 
                                            Assert.equal(taskInfo.parent, null, "task is not parent");
                                            that.zc.releaseTask(taskInfo, true, this); },
                    function () {   that.zc.getTask(['test'], this); },
                    function (taskInfo) { Assert.notEqual(taskInfo, null, "child task not found"); done(); },
                    function () { done(); }
                );
        });
    });

    test('mark child tasks dirty', function (done)
    {
        var level1SearchTags = null;
        var level1AddTags = ['test', 'level1'];
        var level1RemoveTags = [];
        var level1Data = {'level':'1'};
        
        var level2SearchTags = level1AddTags;
        var level2AddTags = ['level2'];
        var level2RemoveTags = ['level1'];
        var level2Data = {'level':'2'};

        var level3SearchTags = level2AddTags;
        var level3AddTags = ['level3'];
        var level3RemoveTags = ['level2'];
        var level3Data = {'level':'3'};

        var level3aSearchTags = level2AddTags;
        var level3aAddTags = ['lev3l'];
        var level3aRemoveTags = ['level2'];
        var level3aData = {'level':'3a'};

        var level4SearchTags = level3AddTags;
        var level4AddTags = ['level4'];
        var level4RemoveTags = ['level3'];
        var level4Data = {'level':'4'};

        Assert.doesNotThrow ( function () {

            Step(
                    function () { that.zc.yes_i_really_want_to_delete_everything(this); },
                    function () { that.zc.addTask(level1Data, level1SearchTags, level1AddTags, level1RemoveTags, false, this); },
                    function () { that.zc.addTask(level2Data, level2SearchTags, level2AddTags, level2RemoveTags, false, this); },
                    function () { that.zc.addTask(level3Data, level3SearchTags, level3AddTags, level3RemoveTags, false, this); },
                    function () { that.zc.addTask(level3aData, level3aSearchTags, level3aAddTags, level3aRemoveTags, false, this); },
                    function () { that.zc.addTask(level4Data, level4SearchTags, level4AddTags, level4RemoveTags, false, this); },
                    function () { that.zc.addTask(level3Data, level3SearchTags, level3AddTags, level3RemoveTags, true, this); },
                    function () { done(); }
                );
        });
    });



});
