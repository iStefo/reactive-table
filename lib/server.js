ReactiveTable = {};

ReactiveTable.publish = function (name, collectionOrFunction, selectorOrFunction, settings) {
  /**
   * Creates the publication that can be used by a ReactiveTable.
   * 
   * @param  {String} publicationId id for this publication (managed by meteor)
   * @param  {String} filter        filter entered by the table user
   * @param  {Array}  fields        the fields used by the table having a `key`
   *                                that describes the mongodb property path
   * @param  {Object} options       hash of `skip`, `limit` and `sort`
   * @param  {Number} rowsPerPage   amount of rows per page
   */
  Meteor.publish("reactive-table-" + name, function (publicationId, filter, fields, options, rowsPerPage) {
    var self = this;

    // prevent update events while handlers are not yet set-up 
    var initializing = true;

    // get collection and selector from argument, which can not only be an
    // object but is also allowed to be a function, which has to be executed
    var collection = _.isFunction(collectionOrFunction) ?
                    collectionOrFunction.call(this) : collectionOrFunction;
    var selector = _.isFunction(selectorOrFunction) ?
                    selectorOrFunction.call(this) : selectorOrFunction;
    
    if (!(collection instanceof Mongo.Collection)) {
      console.log("ReactiveTable.publish: no collection to publish");
      return [];
    }

    // optionally use settings.filterFields rather than the fields shown in
    // the table
    var filterFields;
    if (settings && _.isArray(settings.filterFields)) {
      filterFields = settings.filterFields;
    } else {
      filterFields = fields;
    }

    var filterQuery = _.extend(getFilterQuery(filter, filterFields, settings), selector);

    // if there is a settings object that contains a projection, add it to the
    // mongodb query
    if (settings && settings.fields) {
      options.fields = settings.fields;
    }

    var cursor = collection.find(filterQuery, options);
    console.log(JSON.stringify(filterQuery, null, 4));

    console.log("Found items: " + cursor.count());

    var getRow = function (row, index) {
      return _.extend({
        "reactive-table-id": publicationId,
        "reactive-table-sort": index
      }, row);
    };

    var getRows = function () {
      return _.map(cursor.fetch(), getRow);
    };
    var rows = {};
    _.each(getRows(), function (row) {
      rows[row._id] = row;
    });

    var updateRows = function () {
      var newRows = getRows();
      _.each(newRows, function (row, index) {
        var oldRow = rows[row._id];
        if (oldRow) {
          if (!_.isEqual(oldRow, row)) {
            self.changed("reactive-table-rows-" + publicationId, row._id, row);
            rows[row._id] = row;
          }
        } else {
          self.added("reactive-table-rows-" + publicationId, row._id, row);
          rows[row._id] = row;
        }
      });
    };

    self.added("reactive-table-counts", publicationId, {count: cursor.count()});
    _.each(rows, function (row) {
      self.added("reactive-table-rows-" + publicationId, row._id, row);
    });

    var handle = cursor.observeChanges({
      added: function (id, fields) {
        if (!initializing) {
          self.changed("reactive-table-counts", publicationId, {count: cursor.count()});
          updateRows();
        }
      },

      removed: function (id, fields) {
        self.changed("reactive-table-counts", publicationId, {count: cursor.count()});
        self.removed("reactive-table-rows-" + publicationId, id);
        delete rows[id];
        updateRows();
      },

      changed: function (id, fields) {
        updateRows();
      }

    });

    initializing = false;

    self.ready();

    self.onStop(function () {
      handle.stop();
    });
  });
};
