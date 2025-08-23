import subprocess
import sys
import os
import pyodbc
import pandas as pd

def test_batching_configurations():
    """Test different batching configurations for OrderDetails migration"""
    
    # Test configurations: (batch_size, num_batches, description)
    test_configs = [
        (400, 6, "Smaller batches, more iterations"),
        (800, 3, "Default configuration"),
        (1200, 2, "Larger batches, fewer iterations"),
        (1600, 1, "Single large batch")
    ]
    
    connection_string = ("DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1;"
                        "DATABASE=master;UID=sa;PWD=Password123!;TrustServerCertificate=yes;")
    
    results = []
    
    for batch_size, num_batches, description in test_configs:
        print(f"\n=== Testing: {description} (batch_size={batch_size}, num_batches={num_batches}) ===")
        
        try:
            # Run migration with specific batch configuration
            cmd = [
                sys.executable, "../migrator.py",
                "--batch-size", str(batch_size),
                "--num-batches", str(num_batches)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
            
            if result.returncode == 0:
                print("✓ Migration completed successfully")
                
                # Verify data was migrated correctly
                try:
                    conn = pyodbc.connect(connection_string)
                    cursor = conn.cursor()
                    
                    # Count total OrderDetails records
                    cursor.execute("SELECT COUNT(*) FROM [Order Details]")
                    order_details_count = cursor.fetchone()[0]
                    
                    # Count total InvoiceLine records from Chinook
                    cursor.execute("SELECT COUNT(*) FROM InvoiceLine")
                    invoiceline_count = cursor.fetchone()[0]
                    
                    print(f"  OrderDetails count: {order_details_count}")
                    print(f"  Expected count: {invoiceline_count}")
                    
                    if order_details_count == invoiceline_count:
                        print("✓ Data count matches expected")
                        results.append({
                            'config': f"{batch_size}x{num_batches}",
                            'description': description,
                            'status': 'PASS',
                            'order_details_count': order_details_count
                        })
                    else:
                        print("✗ Data count mismatch")
                        results.append({
                            'config': f"{batch_size}x{num_batches}",
                            'description': description,
                            'status': 'FAIL',
                            'order_details_count': order_details_count,
                            'expected_count': invoiceline_count
                        })
                    
                    conn.close()
                    
                except Exception as e:
                    print(f"✗ Error verifying data: {e}")
                    results.append({
                        'config': f"{batch_size}x{num_batches}",
                        'description': description,
                        'status': 'ERROR',
                        'error': str(e)
                    })
            else:
                print(f"✗ Migration failed: {result.stderr}")
                results.append({
                    'config': f"{batch_size}x{num_batches}",
                    'description': description,
                    'status': 'FAIL',
                    'error': result.stderr
                })
                
        except Exception as e:
            print(f"✗ Test execution error: {e}")
            results.append({
                'config': f"{batch_size}x{num_batches}",
                'description': description,
                'status': 'ERROR',
                'error': str(e)
            })
    
    # Print summary
    print("\n" + "="*60)
    print("BATCHING CONFIGURATION TEST SUMMARY")
    print("="*60)
    
    for result in results:
        status_icon = "✓" if result['status'] == 'PASS' else "✗"
        print(f"{status_icon} {result['config']} ({result['description']}): {result['status']}")
        if 'order_details_count' in result:
            print(f"    OrderDetails count: {result['order_details_count']}")
        if 'error' in result:
            print(f"    Error: {result['error']}")
    
    # Check if all tests passed
    all_passed = all(r['status'] == 'PASS' for r in results)
    
    if all_passed:
        print("\n✓ All batching configuration tests passed!")
        return True
    else:
        print(f"\n✗ {len([r for r in results if r['status'] != 'PASS'])} test(s) failed")
        return False

if __name__ == "__main__":
    success = test_batching_configurations()
    sys.exit(0 if success else 1)
