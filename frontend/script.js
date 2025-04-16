const mongoAttributes = [
    "_id", "email", "name", "phone"
];

const neo4jAttributes = [
    "balance", "blocked", "amount", 
    "payment_id", "date", "flagged"
];

window.onload = function () {
    renderCheckboxes("mongodb-attributes", mongoAttributes, "MongoDBUser");
    renderCheckboxes("neo4j-attributes", neo4jAttributes, "Neo4jNodes");
    renderAccountIdCheckbox("mongodb-attributes", "MongoDBUser");
    renderAccountIdCheckbox("neo4j-attributes", "Neo4jNodes");
};

function renderAccountIdCheckbox(containerId, db) {
    const container = document.getElementById(containerId);
    const label = document.createElement("label");
    label.textContent = " account_id";
    
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = "account_id";
    checkbox.name = "attribute";
    checkbox.checked = true;
    checkbox.disabled = true;
    checkbox.dataset.db = db;

    label.prepend(checkbox);
    container.prepend(label);
}

function renderCheckboxes(containerId, attributes, db) {
    const container = document.getElementById(containerId);
    attributes.forEach(attr => {
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.name = "attribute";
        checkbox.value = attr;
        checkbox.dataset.db = db;

        const label = document.createElement("label");
        label.textContent = ` ${attr}`;
        label.prepend(checkbox);

        container.appendChild(label);
        container.appendChild(document.createElement("br"));
    });
}

function submitSelection() {
    const selected = Array.from(document.querySelectorAll('input[name="attribute"]:checked'))
                          .map(el => el.value);

    if (selected.length === 0) {
        alert("Please select at least one attribute.");
        return;
    }

    fetch('http://127.0.0.1:7001/query_data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fields: selected })
    })
    .then(res => res.json())
    .then(response => {
        renderResultsTable(response.results, selected);
    })
    .catch(err => {
        console.error(err);
        alert("Error submitting attributes.");
    });
}

function renderResultsTable(results, selectedFields) {
    selectedFields = [...new Set(selectedFields)];
    const container = document.querySelector(".db-selection");
    const oldTable = document.getElementById("results-table");
    if (oldTable) oldTable.remove();

    const table = document.createElement("table");
    table.id = "results-table";

    const headerRow = document.createElement("tr");
    selectedFields.forEach(field => {
        const th = document.createElement("th");
        th.textContent = field;
        headerRow.appendChild(th);
    });
    table.appendChild(headerRow);

    results.forEach(row => {
        const tr = document.createElement("tr");
        selectedFields.forEach(field => {
            const td = document.createElement("td");
            td.textContent = row[field] !== undefined ? row[field] : "-";
            tr.appendChild(td);
        });
        table.appendChild(tr);
    });

    container.appendChild(table);
}
