let ts0 = +(new Date());
let dataTable;
let loadingComplete = false;
let loadingIndicator;

$(document).ready(function(){
  // Initialize DataTable with empty data first
  dataTable = $('#erica').DataTable({
    data: [], // Start empty, we'll load data via streaming
    initComplete: () => {
      let ts1 = +(new Date());
      console.log(`Initial table setup completed in ${(ts1-ts0)/1000}s`);
      
      // After initialization, start the streaming load process
      const streamer = new DataStreamer({
        firstChunkMinSize: 100,
        progressiveChunkSize: 10000,
      });
      
      streamer.streamJSONL(
        'data/fulltext-eric-records-lite.jsonl.gz',
        // First chunk loaded callback
        ({rows, count, time}) => {
          console.log(`First chunk of ${count} rows loaded in ${time}s`);
          dataTable.rows.add(rows).draw();
        },
        // Progressive chunk loaded callback
        ({rows, count, totalCount, time}) => {
          console.log(`Progressive chunk of ${count} rows loaded (total: ${totalCount}, ${time}s)`);
          dataTable.rows.add(rows).draw(false); // Using false to maintain current paging position
        },
        // All data loaded callback
        ({count, time}) => {
          console.log(`Full data loaded in ${time}s, total ${count} records`);
          loadingComplete = true;
          removeLoadingIndicator();
        },
        // Error callback
        (error) => {
          console.error('Error loading JSONL file:', error);
          showErrorMessage(error);
        },
        // Loading started callback
        () => {
          showLoadingIndicator();
        }
      );
    },
    order: {
      name:'id',
      dir:'asc'
    },
    columns: [
      {data:'id', name:'id', title:'id', width: '100px', render: data => {
        if(data.match(/^E[DJ][0-9]{6,7}$/)) {
          return `<a href="https://web.archive.org/web/2oe_/https://files.eric.ed.gov/fulltext/${data}.pdf" target="_blank">${data}</a>`;
        } else {
          return data;
        }
      }},
      {data:'title', name:'title', title:'title'},
      {data:'author[; ]', name:'author', title:'author'},
      {data:'publicationdateyear', name:'publicationdateyear', title:'year', width: '25px'}
    ],
    processing: true,
    deferRender: true, // Add this for better performance with large datasets
    language: {
      processing: "Loading data..."
    }
  });
});

/**
 * Shows the loading indicator in the UI
 */
function showLoadingIndicator() {
  loadingIndicator = $('<div class="loading-remaining">Loading...</div>');
  $('body').append(loadingIndicator);
}

/**
 * Removes the loading indicator from the UI
 */
function removeLoadingIndicator() {
  if (loadingIndicator) {
    loadingIndicator.remove();
  }
}

/**
 * Displays an error message to the user
 * @param {Error} error - The error that occurred
 */
function showErrorMessage(error) {
  $('body').append(`<div class="error-message" style="color:red">Error loading data: ${error.message}</div>`);
}