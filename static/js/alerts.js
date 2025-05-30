function fetchEmailStatuses() {
    fetch('/get_alerts')
        .then(response => response.json())
        .then(statuses => {
            const statusList = document.getElementById('email-status-list');
            statuses.forEach(status => {
                const listItem = document.createElement('li');
                listItem.textContent = status;
                statusList.insertBefore(listItem, statusList.firstChild);
            });
        })
        .catch(error => console.error('Error:', error));
}
 
fetchEmailStatuses();