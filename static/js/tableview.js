document.addEventListener("DOMContentLoaded", function () {
    fetchDagData();
    function fetchDagData() {
        fetch('/get_dag_data')
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then(data => { 
                let tableBody = document.getElementById('dag-data');
                tableBody.innerHTML = '';  // Clear any previous data

                //"filter-owner", "filter-dag-id", "filter-schedule-interval", "filter-run-id", "filter-run-type",
      // "filter-start-date", "filter-end-date", "filter-state", "filter-duration"


      // <td>${dag_run.external_trigger || 'N/A'}</td>       
      //                   <td>${dag_run.run_type || 'N/A'}</td>
      //                   <td>${dag_run.data_interval_start || 'N/A'}</td>
      //                   <td>${dag_run.data_interval_end || 'N/A'}</td>
      //                   <td>${dag_run.last_scheduling_decision || 'N/A'}</td>
      //                   <td>${dag_run.dag_hash || 'N/A'}</td>
      //                   <td>${dag_run.log_template_id || 'N/A'}</td>

                data.forEach(dag_run => {
                    let row = `<tr>
                        <td>${dag_run.owner || 'N/A'}</td>
                        <td>${dag_run.dag_id || 'N/A'}</td>
                        <td>${dag_run.schedule_interval || 'N/A'}</td>
                        <td>${dag_run.run_id || 'N/A'}</td>
                        <td>${dag_run.run_type || 'N/A'}</td>
                        <td>${dag_run.start_date || 'N/A'}</td>
                        <td>${dag_run.end_date || 'N/A'}</td>        
                        <td>${dag_run.state || 'N/A'}</td>     
                        <td>${dag_run.duration || 'N/A'}</td>    
                    </tr>`;
                    tableBody.innerHTML += row;
                });
            })
            .catch(error => console.error('Error fetching DAG data:', error));
    }

    // Fetch DAG data every 100 seconds
    setInterval(fetchDagData, 100000);

    // Initial fetch when the page loads
    // fetchDagData();
});



function filterTable() {
    let inputIds = [
      "filter-owner", "filter-dag-id", "filter-schedule-interval", "filter-run-id", "filter-run-type",
      "filter-start-date", "filter-end-date", "filter-state", "filter-duration"
    ];
  
    let table = document.getElementById("dag-table");
    let rows = table.getElementsByTagName("tbody")[0].getElementsByTagName("tr");
  
    for (let i = 0; i < rows.length; i++) {
      let row = rows[i];
      let shouldDisplay = true;
  
      for (let j = 0; j < inputIds.length; j++) {
        let filterValue = document.getElementById(inputIds[j]).value.toLowerCase();
        let cellValue = row.getElementsByTagName("td")[j].innerText.toLowerCase();
  
        if (filterValue && cellValue.indexOf(filterValue) === -1) {
          shouldDisplay = false;
          break;
        }
      }
      row.style.display = shouldDisplay ? "" : "none";
    }
  }
  
  // Attach the filter function to each input field
  document.querySelectorAll("input[id^='filter-']").forEach(input => {
    input.addEventListener("keyup", filterTable);
  });
  
  // Function to toggle column visibility based on user preferences
  function toggleColumnVisibility() {
    let checkboxes = document.querySelectorAll(".column-checkbox");
    let table = document.getElementById("dag-table");
  
    checkboxes.forEach(checkbox => {
      let column = checkbox.getAttribute("data-column");
      let isVisible = checkbox.checked;
  
      // Show or hide the header
      let th = table.getElementsByTagName("th")[column];
      th.style.display = isVisible ? "" : "none";
  
      // Show or hide the column data for all rows
      let rows = table.getElementsByTagName("tbody")[0].getElementsByTagName("tr");
      for (let i = 0; i < rows.length; i++) {
        let td = rows[i].getElementsByTagName("td")[column];
        td.style.display = isVisible ? "" : "none";
      }
    });
  }
  
  // Attach event listener to checkboxes for column visibility
  document.querySelectorAll(".column-checkbox").forEach(checkbox => {
    checkbox.addEventListener("change", toggleColumnVisibility);
  });
  
  // Initialize visibility based on initial checkbox states when the page loads
  window.onload = toggleColumnVisibility;





  function downloadCSV() {
    const table = document.getElementById('dag-table');
    let csvContent = "data:text/csv;charset=utf-8,";

    // Get table headers
    const headers = Array.from(table.rows[0].cells).map(cell => cell.textContent.trim());
    csvContent += headers.join(",") + "\n";

    // Get visible rows (excluding the hidden ones)
    for (let i = 1; i < table.rows.length; i++) {
        const row = table.rows[i];
        if (row.style.display !== 'none') {
            const rowData = Array.from(row.cells).map(cell => cell.textContent.trim());
            csvContent += rowData.join(",") + "\n";
        }
    }

    // Download the CSV
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', 'dag_data.csv');
    document.body.appendChild(link); // Required for FF
    link.click();
    document.body.removeChild(link); // Cleanup
}

// Add event listener for the CSV download button
document.getElementById('download-csv').addEventListener('click', downloadCSV);