function triggerReschedule(strategy) {
    fetch(`/reschedule_dags/${strategy}`, {
        method: 'POST'
    })
    .then(response => {
        if (!response.ok) {
            // Log the response for debugging
            return response.text().then(text => { throw new Error(`HTTP error! status: ${response.status}, response: ${text}`); });
        }
        return response.json();
    })
    .then(data => {
        document.getElementById('status').innerHTML = `<p>${data.message}</p>`;
    })
    .catch(error => {
        document.getElementById('status').innerHTML = `<p>Error: ${error.message}</p>`;
    });
}