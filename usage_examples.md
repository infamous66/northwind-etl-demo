# Usage Examples for Configurable Batching

The migrator.py script now supports configurable batching parameters for OrderDetails data migration.

## Command Line Parameters

- `--batch-size`: Number of records to process in each batch (default: 800)
- `--num-batches`: Number of batches to process (default: 3)

## Examples

### Default Configuration
```bash
python migrator.py
```
This uses the default settings: batch size of 800 and 3 batches (total: 2400 records)

### Custom Batch Size
```bash
python migrator.py --batch-size 500
```
This processes 500 records per batch with 3 batches (total: 1500 records)

### Custom Number of Batches
```bash
python migrator.py --num-batches 5
```
This processes 800 records per batch with 5 batches (total: 4000 records)

### Both Parameters
```bash
python migrator.py --batch-size 1000 --num-batches 2
```
This processes 1000 records per batch with 2 batches (total: 2000 records)

### Small Batches for Testing
```bash
python migrator.py --batch-size 100 --num-batches 10
```
This processes 100 records per batch with 10 batches (total: 1000 records)

### Large Single Batch
```bash
python migrator.py --batch-size 2000 --num-batches 1
```
This processes all records in a single batch of 2000

## GitHub Actions

The GitHub workflow automatically runs with the default configuration:
```yaml
- name: Run data migration with migrator.py
  run: python ./migrator.py --batch-size 800 --num-batches 3
```

## Testing Different Configurations

The `tests/test_batching.py` script automatically tests multiple configurations:
- 400 records × 6 batches
- 800 records × 3 batches (default)
- 1200 records × 2 batches  
- 1600 records × 1 batch

Run the batching tests locally:
```bash
cd tests/
python test_batching.py
```

## Dry-Run Testing

Before running the actual migration, you can use the dry-run script to see how the batching will work:

```bash
# Test default configuration
python dry_run.py

# Test custom configuration
python dry_run.py --batch-size 500 --num-batches 4

# Test with different database connection
python dry_run.py --batch-size 1000 --num-batches 2 --mysql-host myhost --mysql-user myuser
```

The dry-run script will:
- Show the batch breakdown
- Display the SQL queries that would be executed
- Check if your configuration matches the available data
- Not perform any actual migration

## Notes

- The total number of records processed = batch_size × num_batches
- Make sure the total doesn't exceed the actual number of InvoiceLine records in your Chinook database
- Larger batch sizes may use more memory but fewer database round trips
- Smaller batch sizes use less memory but more database round trips
- Use the dry-run script to validate your configuration before running the actual migration
