/**
 * Test dependencies
 */
var Adapter = require('../../');


describe('registerCollection', function () {

	it('should not hang or encounter any errors', function (done) {
		Adapter.registerCollection({
			identity: 'foo'
		}, done);
	});

    it('should create a model', function( done ){

        console.log(Adapter);

        done();
    });
});