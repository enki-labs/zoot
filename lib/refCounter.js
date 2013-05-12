var Assert = require('assert');

/**
* Basic ref counter for tracking async calls.
*/
var RefCounter = function (done)
{
    this.counter = 0;
    this.done = done;
};

/**
* Start a call.
*/
RefCounter.prototype.start = function ()
{
    this.counter++;
};

/**
* End a call.
*/
RefCounter.prototype.end = function ()
{
    this.counter--;
    Assert.ok(this.counter >= 0, "invalid reference release");
    if (this.counter == 0) this.done();
};

/**
* Export classes and modules.
*/
module.exports = RefCounter;
