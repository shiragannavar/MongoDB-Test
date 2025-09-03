let userToDelete = null;

function deleteUser(userId) {
    userToDelete = userId;
    const modal = new bootstrap.Modal(document.getElementById('deleteModal'));
    modal.show();
}

document.getElementById('confirmDelete').addEventListener('click', function() {
    if (userToDelete) {
        fetch(`/delete_user/${userToDelete}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Remove the user row from the table
                const userRow = document.getElementById(`user-${userToDelete}`);
                if (userRow) {
                    userRow.remove();
                }
                
                // Show success message
                showAlert('User deleted successfully!', 'success');
                
                // Check if table is now empty
                const tbody = document.querySelector('tbody');
                if (tbody && tbody.children.length === 0) {
                    location.reload(); // Reload to show empty state
                }
            } else {
                showAlert('Error deleting user: ' + data.message, 'danger');
            }
        })
        .catch(error => {
            showAlert('Error deleting user: ' + error.message, 'danger');
        })
        .finally(() => {
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('deleteModal'));
            modal.hide();
            userToDelete = null;
        });
    }
});

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
                    
                    // Reload page to refresh user list from new database
                    setTimeout(() => {
                        location.reload();
                    }, 1500);
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
