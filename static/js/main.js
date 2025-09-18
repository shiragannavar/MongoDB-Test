
// Database toggle functionality
document.addEventListener('DOMContentLoaded', function() {
    const dbToggle = document.getElementById('dbToggle');
    if (dbToggle) {
        dbToggle.addEventListener('change', function() {
            const newDbType = this.checked ? 'hcd' : 'mongodb';
            
            // Show loading state
            showAlert('Switching database...', 'info');
            
            fetch('/api/switch_database', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    database_type: newDbType
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Update UI elements
                    const dbBadge = document.getElementById('dbBadge');
                    const dbTypeSpan = document.getElementById('dbType');
                    
                    if (dbBadge) {
                        if (newDbType === 'hcd') {
                            dbBadge.textContent = 'DataStax HCD';
                            dbBadge.className = 'badge bg-info';
                        } else {
                            dbBadge.textContent = 'MONGODB';
                            dbBadge.className = 'badge bg-success';
                        }
                    }
                    
                    if (dbTypeSpan) {
                        if (newDbType === 'hcd') {
                            dbTypeSpan.textContent = 'DataStax HCD';
                        } else {
                            dbTypeSpan.textContent = 'Mongodb';
                        }
                    }
                    
                    showAlert(data.message, 'success');
                    
                    // Refresh subscriber data from new database
                    setTimeout(() => {
                        refreshSubscriberData();
                    }, 1000);
                } else {
                    // Revert toggle state on error
                    dbToggle.checked = !dbToggle.checked;
                    showAlert('Error switching database: ' + data.message, 'danger');
                }
            })
            .catch(error => {
                // Revert toggle state on error
                dbToggle.checked = !dbToggle.checked;
                showAlert('Error switching database: ' + error.message, 'danger');
            });
        });
    }
});

function updateDatabaseStatus(dbType) {
    const dbBadge = document.getElementById('dbBadge');
    const dbTypeSpan = document.getElementById('dbType');
    
    if (dbBadge) {
        dbBadge.textContent = dbType.toUpperCase();
        dbBadge.className = `badge bg-${dbType === 'mongodb' ? 'success' : 'info'}`;
    }
    
    if (dbTypeSpan) {
        dbTypeSpan.textContent = dbType.charAt(0).toUpperCase() + dbType.slice(1);
    }
}

function showAlert(message, type) {
    // Create alert container if it doesn't exist
    let alertContainer = document.querySelector('.alert-container');
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.className = 'alert-container';
        document.body.appendChild(alertContainer);
    }
    
    // Map types to custom classes
    const typeClass = type === 'success' ? 'alert-success-custom' : 
                     type === 'danger' ? 'alert-danger-custom' : 
                     `alert-${type}`;
    
    // Create alert element
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert ${typeClass} alert-notification alert-dismissible fade show`;
    
    // Add icon based on type
    const icon = type === 'success' ? 'fas fa-check-circle' : 
                type === 'danger' ? 'fas fa-exclamation-triangle' : 
                'fas fa-info-circle';
    
    alertDiv.innerHTML = `
        <div class="d-flex align-items-center">
            <i class="${icon} alert-icon me-2"></i>
            <span>${message}</span>
            <button type="button" class="btn-close ms-auto" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Add to container
    alertContainer.appendChild(alertDiv);
    
    // Auto-dismiss after 4 seconds
    setTimeout(() => {
        if (alertDiv && alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 4000);
}


function syncSubscribersToHcd() {
    const syncBtn = document.getElementById('syncSubscribersToHcdBtn');
    
    // Disable button and show loading state
    syncBtn.disabled = true;
    syncBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Syncing 100 Records...';
    
    fetch('/api/sync_subscribers_to_hcd', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            let message = data.message;
            if (data.synced_count !== undefined) {
                message += ` (${data.synced_count} subscribers synced)`;
            }
            showAlert(message, 'success');
        } else {
            showAlert('Subscriber sync failed: ' + data.message, 'danger');
        }
    })
    .catch(error => {
        showAlert('Subscriber sync failed: ' + error.message, 'danger');
    })
    .finally(() => {
        // Re-enable button
        syncBtn.disabled = false;
        syncBtn.innerHTML = '<i class="fas fa-sync me-1"></i>Sync 100 to HCD';
    });
}

// Subscriber management functions
function createSampleSubscriber() {
    fetch('/api/subscribers/sample', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showAlert('Sample subscriber created successfully!', 'success');
            setTimeout(() => {
                location.reload();
            }, 1500);
        } else {
            showAlert('Error creating subscriber: ' + data.error, 'danger');
        }
    })
    .catch(error => {
        showAlert('Error creating subscriber: ' + error.message, 'danger');
    });
}

function refreshSubscribers() {
    showAlert('Refreshing subscriber data...', 'info');
    setTimeout(() => {
        location.reload();
    }, 500);
}

function viewSubscriber(hashMsisdn) {
    fetch(`/api/subscribers/${hashMsisdn}`)
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            const subscriber = data.subscriber;
            
            // Create modal content
            const modalContent = `
                <div class="modal fade" id="subscriberModal" tabindex="-1">
                    <div class="modal-dialog modal-lg">
                        <div class="modal-content">
                            <div class="modal-header">
                                <h5 class="modal-title">Subscriber Details</h5>
                                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body">
                                <div class="row">
                                    <div class="col-md-6">
                                        <h6>Basic Information</h6>
                                        <p><strong>Hash MSISDN:</strong> <small class="font-monospace">${subscriber.hashMsisdn}</small></p>
                                        <p><strong>Provider:</strong> ${subscriber.provider}</p>
                                        <p><strong>Subscription Type:</strong> ${subscriber.subscriptionType}</p>
                                        <p><strong>Status:</strong> <span class="badge bg-${subscriber.status === 'A' ? 'success' : 'warning'}">${subscriber.status === 'A' ? 'Active' : 'Inactive'}</span></p>
                                        <p><strong>Circle ID:</strong> ${subscriber.circleID}</p>
                                        <p><strong>Active MSISDN:</strong> ${subscriber.activeMsisdn}</p>
                                    </div>
                                    <div class="col-md-6">
                                        <h6>Activity Information</h6>
                                        <p><strong>First Recharge:</strong> ${subscriber.firstRechargeDate || 'N/A'}</p>
                                        <p><strong>Last Login:</strong> ${subscriber.lastLoginDate || 'N/A'}</p>
                                        <p><strong>Last Login Channel:</strong> ${subscriber.lastLoginChannel || 'N/A'}</p>
                                        <p><strong>Date of Storage:</strong> ${subscriber.dateofStorage || 'N/A'}</p>
                                    </div>
                                </div>
                                
                                <hr>
                                
                                <div class="row">
                                    <div class="col-md-6">
                                        <h6>Products (${subscriber.subscribedProductOffering?.product?.length || 0})</h6>
                                        <div class="table-responsive">
                                            <table class="table table-sm">
                                                <thead>
                                                    <tr><th>Name</th><th>Status</th><th>Type</th></tr>
                                                </thead>
                                                <tbody>
                                                    ${subscriber.subscribedProductOffering?.product?.map(p => 
                                                        `<tr><td>${p.name}</td><td><span class="badge bg-${p.status === 'A' ? 'success' : 'warning'}">${p.status}</span></td><td>${p.productType}</td></tr>`
                                                    ).join('') || '<tr><td colspan="3">No products</td></tr>'}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <h6>Services (${subscriber.subscribedProductOffering?.services?.length || 0})</h6>
                                        <div class="table-responsive">
                                            <table class="table table-sm">
                                                <thead>
                                                    <tr><th>Name</th><th>State</th><th>Type</th></tr>
                                                </thead>
                                                <tbody>
                                                    ${subscriber.subscribedProductOffering?.services?.map(s => 
                                                        `<tr><td>${s.name}</td><td><span class="badge bg-${s.state === 'A' ? 'success' : 'warning'}">${s.state}</span></td><td>${s.serviceType}</td></tr>`
                                                    ).join('') || '<tr><td colspan="3">No services</td></tr>'}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            // Remove existing modal if any
            const existingModal = document.getElementById('subscriberModal');
            if (existingModal) {
                existingModal.remove();
            }
            
            // Add modal to body
            document.body.insertAdjacentHTML('beforeend', modalContent);
            
            // Show modal
            const modal = new bootstrap.Modal(document.getElementById('subscriberModal'));
            modal.show();
            
        } else {
            showAlert('Error loading subscriber details: ' + data.message, 'danger');
        }
    })
    .catch(error => {
        showAlert('Error loading subscriber details: ' + error.message, 'danger');
    });
}

function deleteSubscriber(hashMsisdn) {
    if (confirm('Are you sure you want to delete this subscriber? This action cannot be undone.')) {
        fetch(`/api/subscribers/${hashMsisdn}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Remove the subscriber row from the table
                const subscriberRow = document.getElementById(`subscriber-${hashMsisdn}`);
                if (subscriberRow) {
                    subscriberRow.remove();
                }
                
                showAlert('Subscriber deleted successfully!', 'success');
                
                // Check if table is now empty
                const tbody = document.querySelector('#subscriber-table tbody');
                if (tbody && tbody.children.length === 0) {
                    location.reload(); // Reload to show empty state
                }
            } else {
                showAlert('Error deleting subscriber: ' + data.message, 'danger');
            }
        })
        .catch(error => {
            showAlert('Error deleting subscriber: ' + error.message, 'danger');
        });
    }
}
