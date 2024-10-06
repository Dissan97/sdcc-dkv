function sendPut() {
    const key = document.getElementById("putMessageKey").value;
    const value = document.getElementById("putMessageValue").value;

    fetch('/put', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ key: key, value: value })
    })
        .then(response => {
            if (response.ok) {
                return response.text(); // Get the response text
            } else {
                throw new Error("Error logging message.");
            }
        })
        .then(async data => {
            // Display the response message
            document.getElementById("response").innerText = await data;
        })
        .catch(error => {
            console.error("Error:", error);
            document.getElementById("response").innerText = error.message; // Display the error message
        });
}

function sendGet() {
    const key = document.getElementById("getMessageKey").value;

    fetch(`/get?key=${encodeURIComponent(key)}`, {
        method: 'GET'
    })
        .then(response => {
            if (response.ok) {
                return response.text(); // Get the response text
            } else {
                throw new Error("Error retrieving message.");
            }
        })
        .then(async data => {
            // Display the response message
            document.getElementById("response").innerText = await data;
        })
        .catch(error => {
            console.error("Error:", error);
            document.getElementById("response").innerText = error.message; // Display the error message
        });
}

function sendDelete() {
    const key = document.getElementById("delMessageKey").value;

    fetch('/delete', {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ key: key })
    })
        .then(response => {
            if (response.ok) {
                return response.text(); // Get the response text
            } else {
                throw new Error("Error deleting message.");
            }
        })
        .then(async data => {
            // Display the response message
            document.getElementById("response").innerText = await data;
        })
        .catch(error => {
            console.error("Error:", error);
            document.getElementById("response").innerText = error.message; // Display the error message
        });
}
