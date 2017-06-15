(function() {

  var myConnector = tableau.makeConnector();
  var schema = [];
  var server = 'localhost';
  var port = 8086;
  var db = '';
  var debug = true; // set to true to enable JS Console debug messages
  var protocol = "http://"; // default to non-encrypted.  To setup InfluxDB with https see https://docs.influxdata.com/influxdb/v1.2/administration/https_setup/
  var useAuth = false; // bool to include/prompt for username/password
  var username = "";
  var password = "";
  var queryString_Auth; // string to hold the &u=_user_&p=_pass_ part of the query string
  var queryString_Auth_Log; // use for logging the redacted password

  var useAggregation = true; // use aggregation in the queries?
  var interval_time = '30'; // value for the group by time
  var interval_measure = 'm'; // h=hour, m=min, etc
  var aggregation = 'mean'; // value for aggregating database value

  // From https://stackoverflow.com/questions/4656843/jquery-get-querystring-from-url
  // Read a page's GET URL variables and return them as an associative array.
  function getUrlVars() {
    var vars = [],
      hash;
    var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
    for (var i = 0; i < hashes.length; i++) {
      hash = hashes[i].split('=');
      vars.push(hash[0]);
      vars[hash[0]] = hash[1];
    }
    return vars;
  }

  function resetSchema() {
    schema = [];
    console.log("Schema has been reset");
  }
  resetSchema();

  function queryStringTags(index, queryString_tags) {
    if (debug) console.log('Retrieving tagis with query: %s', queryString_tags);
    // Create a JQuery Promise object
    var deferred = $.Deferred();
    $.ajaxSetup({
      crossOrigin: true
    });
    $.getJSON(queryString_tags, function(tags) {
      if (debug) console.log('tag query string for ' + index + ": " + JSON.stringify(tags));

      // Create a factory (array) of async functions
      var deferreds = (tags.results[0].series[0].values).map(function(tag, tag_index) {
        if (debug) console.log("in queryStringTags.  tag: %s  tag_index: %s", tag[0], tag_index);
        schema[index].columns.push({
          id: tag[0],
          dataType: tableau.dataTypeEnum.string
        });
        if (debug) console.log(schema);
      });

      // Execute all async functions in array
      return $.when.apply($, deferreds)
        .then(function() {
          if (debug) console.log('finished processing tags');
          deferred.resolve();
        });
    });
    return deferred.promise();

  }

  function queryStringFields(index, queryString_fields) {
    var deferred = $.Deferred();
    if (debug) console.log('Retrieving fields with query: %s', queryString_fields);
    $.ajaxSetup({
      crossOrigin: true
    });
    $.getJSON(queryString_fields, function(fields) {
      var deferreds = (fields.results[0].series[0].values).map(function(field, field_index) {
        if (debug) console.log("in queryStringFields.  field: %s  field_index: %s", field[0], field_index);
        var id_str;
        if (useAggregation) {
          id_str = aggregation + '_' + field[0];
        } else {
          id_str = field[0];
        }
        // force the correct mapping of data types
        var tabDataType;
        switch (field[1]) {
          case 'float':
            tabDataType = tableau.dataTypeEnum.float;
            break;
          case 'integer':
            tabDataType = tableau.dataTypeEnum.int;
            break;
          case 'string':
            tabDataType = tableau.dataTypeEnum.string;
            break;
          case 'boolean':
            tabDataType = tableau.dataTypeEnum.bool;
            break;
        }
        schema[index].columns.push({
          id: id_str,
          dataType: tabDataType
        });
      });
      return $.when.apply($, deferreds)
        .then(function() {
          if (debug) console.log('finished processing fields');
          deferred.resolve();
        });
    });
    return deferred.promise();
  }

  function addTimeTag() {
    // the "time" tag isn't returnedy by the schema.  Add it to every measurement.
    $.each(schema, function(index, e) {
      if (debug) console.log("e: %s   index: %s", e, index);

      schema[index].columns.unshift({
        id: 'time',
        dataType: tableau.dataTypeEnum.datetime
      });
    });
  }


  function getMeasurements(db, queryString) {
    // Get all measurements (aka Tables) from the DB

    $.ajaxSetup({
      crossOrigin: true
    });

    $.getJSON(queryString, function(resp) {
      if (debug) console.log('retrieved all measurements: %o', resp);
      if (debug) console.log('resp.results[0].series[0].values: %o', resp.results[0].series[0].values);

      // for each measurement, save the async function to a "factory" array
      var deferreds = (resp.results[0].series[0].values).map(function(measurement, index) {
        schema[index] = {
          id: measurement[0],
          incrementColumnId: "time",
          columns: []
        };
        if (debug) console.log(schema);
        if (debug) console.log("analyzing index: %s measurement: %s ", index, measurement[0]);
        if (debug) console.log("schema now is: %o", schema);

        //
        var deferred_tags_and_fields = [];

        // Get the tags (items that can be used in a where clause) in the measurement
        var queryString_tags = protocol + server + ":" + port + "/query?q=SHOW+TAG+KEYS+FROM+%22" + measurement + "%22&db=" + db;
        if (useAuth) {
          queryString_tags += queryString_Auth;
        }

        // Get fields/values
        var queryString_fields = protocol + server + ":" + port + "/query?q=SHOW+FIELD+KEYS+FROM+%22" + measurement + "%22&db=" + db;
        if (useAuth) {
          queryString_fields += queryString_Auth;
        }

        deferred_tags_and_fields.push(queryStringTags(index, queryString_tags));
        deferred_tags_and_fields.push(queryStringFields(index, queryString_fields));

        return $.when.apply($, deferred_tags_and_fields)
          .then(function() {
            if (debug) console.log('finished processing queryStringTags and queryStringFields for %s', measurement[0]);
          });



      });

      return $.when.apply($, deferreds)
        .then(function() {
          if (debug) console.log('Finished getting ALL tags and fields for ALL measurements.  Hooray!');
          if (debug) console.log("schema is now: %o", schema);
        })
        .then(addTimeTag)
        .then(function() {
          if (debug) console.log("schema finally: %o", schema);

          // Once we have the tags/fields enable the Load button
          tableauSubmit();
        });
    });
  }



  function getDBs() {

    $('.dropdown-menu li a').click(function() {
      var _which = $(this).closest("ul").attr('id'); // get the ID of the UL element
      if (debug) console.log(_which + " changed to: " + $(this).text());
      $(this).parents(".dropdown").find('.btn').html($(this).text() + ' <span class="caret"></span>');
      $(this).parents(".dropdown").find('.btn').val($(this).data('value'));

      if (_which === "protocol_selector") {
        protocol = $(this).text();
      } else if (_which === "aggregation_selector") {
        aggregation = $(this).text();
      } else if (_which === "interval_measure") {
        interval_measure = $(this).data('prefix');
      }

    });

    $('#interval_time').change(function() {
      if ($(this).val() === "") {
        interval_time = $(this).prop('placeholder');
      } else {
        interval_time = $(this).val();
      }
    });

    // retrieve the list of databases from the server
    $('#tableButton').click(function() {

      // Reset the dropdown in case the user selects another server
      $('.selectpicker').html('');
      $('.selectpicker').selectpicker('refresh');


      if ($('#servername').val() !== "") {
        server = $('#servername').val();
      } else {
        server = "localhost";
      }

      if ($('#serverport').val() !== "") {
        port = $('#serverport').val();
      } else {
        port = 8086;
      }

      var queryString_DBs = protocol + server + ":" + port + "/query?q=SHOW+DATABASES";
      if (useAuth) {
        username = $('#username').val();
        password = $('#password').val();
        queryString_Auth = "&u=" + username + "&p=" + password;
        queryString_Auth_Log = "&u=" + username + "&p=[redacted]";
        queryString_DBs += queryString_Auth;
      }

      if (debug) console.log("Retrieving databases with querystring: ", queryString_DBs);
      $.ajaxSetup({
        crossOrigin: true
      });
      $.getJSON(queryString_DBs, function(resp) {
          if (debug) console.log(resp.results[0].series[0].values);

          $('.selectpicker').html('');
          $.each(resp.results[0].series[0].values, function(index, value) {
            $('<option>' + value + '</option>').appendTo('.selectpicker');
          });
          $('.selectpicker').selectpicker('refresh');

          // Once we have the databases, enable the 'load schema' button
          $('#getSchemaButton').prop('disabled', false);
        })
        .done(function() {
          //alert("done")
        })
        .fail(function(err) {
          console.log(err);
          $('#influx_alert').html("<div class='alert alert-error'><strong>Error loading database</strong>" + JSON.stringify(err) + "</div>");
          $('#influx_alert').fadeIn();
        });
    });

    $('#db_dropdown').on('changed.bs.select', function(e) {
      if (debug) console.log(e.target.value + " has been selected");

      // reset the schema if the database selection changes
      resetSchema();
    });

    $('#getSchemaButton').click(function() {
      db = $('#db_dropdown option:selected').text();
      var queryString = protocol + server + ":" + port + "/query?q=SHOW+MEASUREMENTS&db=" + db;
      if (useAuth) {
        queryString += queryString_Auth;
      }
      getMeasurements(db, queryString);
    });
  }


  function tableauSubmit() {
    tableau.connectionName = "InfluxDB";
    var json = {
      db: db,
      server: server,
      aggregation: aggregation,
      interval_time: interval_time,
      interval_measure: interval_measure,
      protocol: protocol,
      port: port,
      useAuth: useAuth,
      useAggregation: useAggregation,
      schema: schema,
    };
    if (useAuth) {
      tableau.username = username;
      tableau.password = password;
    }
    tableau.connectionData = JSON.stringify(json);
    tableau.submit();
  }


  // Init function for connector, called during every phase
  myConnector.init = function(initCallback) {
    if (debug) console.log('Calling init function in phase: ', tableau.phase);
    if (useAuth) {
      tableau.authType = tableau.authTypeEnum.basic;
    } else {
      tableau.authType = tableau.authTypeEnum.none;
    }
    initCallback();
  };

  myConnector.getSchema = function(schemaCallback) {
    var json = JSON.parse(tableau.connectionData);
    schemaCallback(json.schema);
  };

  myConnector.getData = function(table, doneCallback) {

    if (debug) {
      console.log(table);
      console.log('lastId (for incremental refresh): ', table.incrementValue);
      console.log("Using Auth: " + useAuth);
    }
    var lastId = table.incrementValue || -1;

    var tableData = [];
    var json = JSON.parse(tableau.connectionData);
    var queryString = json.protocol + json.server + ":" + json.port + "/query?q=select+";

    if (json.useAggregation) {
      queryString += json.aggregation + "(*)";
    } else {
      queryString += '*';
    }
    queryString += "+from+" + table.tableInfo.id;
    if (json.useAggregation) {
      if (lastId !== -1) {
        // incremental refresh with aggregation
        queryString += "+where+time+%3E+'" + lastId + "'+group+by+*,time(" + json.interval_time + json.interval_measure + ")";
      } else {
        // full refresh with aggregation
        queryString += "+where+time+<+now()+group+by+*,time(" + json.interval_time + json.interval_measure + ")";
      }
    } else {
      if (lastId !== -1) {
        // incremental refresh with NO aggregation
        queryString += "+where+time+%3E+'" + lastId + "'";
      } else {
        // full refresh with NO aggregation
      }
    }
    queryString += "&db=" + json.db; // + "&chunked=true&chunk_size=20000";
    if (json.useAuth) {
      queryString += "&u=" + tableau.username + "&p=" + tableau.password;
    }
    if (debug) console.log("Fetch data query string: ", queryString);
    $.ajaxSetup({
      crossOrigin: true
    });
    $.getJSON(queryString, function(resp) {
        var values, columns, tags, val, val_len, col, col_len, series, series_cnt, row;

        // Need this line for incremental refresh.  If there are no additional results than this set will be undefined.
        if ((resp.results[0]).hasOwnProperty('series') === true) {

          if (!json.useAggregation) {

            values = resp.results[0].series[0].values;
            columns = resp.results[0].series[0].columns;

            if (debug) {
              console.log("columns:", columns);
              console.log("first row of values: ", values[0]);
              console.log("Total # of rows for " + table.tableInfo.id + " is: " + values.length);
              console.log("Using Aggregation: " + json.useAggregation);
            }

            //Iterate over the result set
            for (val = 0, val_len = values.length; val < val_len; val++) {
              row = {};
              for (col = 0, col_len = columns.length; col < col_len; col++) {
                row[columns[col]] = values[val][col];
              }
              tableData.push(row);
              if (val % 20000 === 0) {
                console.log("Getting data: " + val + " rows.");
                tableau.reportProgress("Getting data: " + val + " rows.");
              }
            }
          } else {

            series = resp.results[0].series;

            if (debug) {
              console.log("first row of tags:", series[0].tags);
              console.log("first row of columns:", series[0].columns);
              console.log("first row of values: ", series[0].values);
              console.log("Total # of series (%s) & columns (%s) & values (1st row: %s) = total rows (est: %s) for %s is: ", series.length, series[0].columns.length, series[0].values.length, series.length * series[0].columns.length * series[0].values.length, table.tableInfo.id);
            }

            //Iterate over the result set
            for (series_cnt = 0, series_len = series.length; series_cnt < series_len; series_cnt++) {


              values = series[series_cnt].values;
              for (val = 0, val_len = values.length; val < val_len; val++) {
                columns = series[series_cnt].columns;
                row = {};

                // add tags from each series
                var obj = series[series_cnt].tags;
                for (var key in obj) {
                  if (obj.hasOwnProperty(key)) {
                    row[key] = obj[key];
                  }
                }
                for (col = 0, col_len = columns.length; col < col_len; col++) {

                  // console.log("val=%s  col=%s", val, col)
                  // console.log("row[series[series_cnt].columns[col]]:", series[series_cnt].columns[col])
                  // console.log("series[series_cnt].values[val][col]", series[series_cnt].values[val][col])
                  row[series[series_cnt].columns[col]] = series[series_cnt].values[val][col];
                }

                tableData.push(row);
                if (val % 20000 === 0) {
                  console.log("Getting data: " + val + " rows.");
                  tableau.reportProgress("Getting data: " + val + " rows.");
                }
              }
            }

          }

        } else {
          if (debug) console.log('No additional data in table ' + table.tableInfo.id + ' or in incremental refresh after ', table.incrementValue);
        }


      }).done(function() {
        console.log("Finished getting table data for " + table.tableInfo.id);
        table.appendRows(tableData);
        doneCallback();
      })
      .fail(function(err) {
        tableau.abortWithError(JSON.stringify(err));
        console.log(err);
        doneCallback();
      });
  };



  $(document).ready(function() {
    var urlVars = getUrlVars();
    console.log("currentURL: " + window.location.href + "  urlVars[auth]: " + urlVars.auth);
    if (urlVars.auth === 'true' || urlVars.auth === true) {
      useAuth = true;
    } else {
      useAuth = false;
    }

    if (useAuth === true) {
      $('#authGroup').removeClass('hidden');
      $('#useAuthCheckBox').attr("checked", "checked");
      $('#reloadWithAuth').attr("hidden", "hidden");

    } else {
      $('#authGroup').addClass('hidden');
      $('#useAuthCheckBox').attr("checked");
      $('#reloadWithoutAuth').attr("hidden", "hidden");
    }

    if (useAggregation === true) {
      $('#aggregationGroup').removeClass('hidden');
      $('#useAggregationCheckBox').attr("checked", "checked");
    } else {
      $('#aggregationGroup').addClass('hidden');
      $('#useAggregationCheckBox').attr("checked");
    }

    $('#useAuthCheckBox').click(function() {
      console.log("url:" + window.location.href + " indexof(?): " + window.location.href.indexOf('?') + " window.slice: " + window.location.href.slice(0, window.location.href.indexOf('?') + 1));
      var _url;
      if (window.location.href.indexOf('?') === -1) {
        _url = window.location.href;
        console.log("_url (-1):" + _url);
      } else {
        _url = window.location.href.slice(0, window.location.href.indexOf('?'));
        console.log("_url (#):" + _url);

      }

      if ($(this).attr("checked") === undefined) {
        _url += "?auth=true";
      } else {
        _url += "?auth=false";

      }
      console.log("Will open: " + _url);
      window.open(_url, 'wdc', '_self');

    });


    $('#useAggregationCheckBox').click(function() {

      if ($(this).prop("checked") === false) {
        $('#aggregationGroup').addClass('hidden');
        useAggregation = false;
      } else {
        $('#aggregationGroup').removeClass('hidden');
        useAggregation = true;
      }
    });



    getDBs();
    tableau.registerConnector(myConnector);
  });


})();
