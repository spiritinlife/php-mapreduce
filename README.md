# PHP MapReduce

A high-performance, framework-agnostic MapReduce implementation for PHP that processes large datasets using parallel workers and disk-based storage.

[![PHP Version](https://img.shields.io/badge/php-%5E8.1-blue)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Why Use This?

- Process datasets larger than RAM using memory-efficient disk storage
- **Stream iterators without loading into memory** - process large files, database cursors, and API responses efficiently
- Utilize multiple CPU cores for true parallel processing
- Framework-agnostic - works with any PHP project
- Handle millions of records with predictable memory usage

## Installation

```bash
composer require spiritinlife/php-mapreduce
```

**Requirements:** PHP 8.1+

## Quick Start

Word count example:

```php
use Spiritinlife\MapReduce\MapReduceBuilder;

$documents = [
    'doc1' => 'the quick brown fox',
    'doc2' => 'the lazy dog',
    'doc3' => 'quick brown dogs',
];

$results = (new MapReduceBuilder())
    ->input($documents)
    ->map(function ($docId, $text) {
        foreach (str_word_count(strtolower($text), 1) as $word) {
            yield [$word, 1];
        }
    })
    ->reduce(function ($word, $counts) {
        return array_sum($counts);
    })
    ->execute();

// Output: ['quick' => 2, 'brown' => 2, 'the' => 2, ...]
```

## How It Works

MapReduce processes data in three parallel phases:

1. **Map Phase** - Input is split and processed in parallel. Each worker emits key-value pairs to partitioned temporary files.
2. **Shuffle Phase** - Partitions are sorted and grouped in parallel using memory-efficient external sorting.
3. **Reduce Phase** - Partitions are processed in parallel, aggregating values by key.

```
Input → [Parallel Map] → [Parallel Shuffle & Sort] → [Parallel Reduce] → Results
```

## Input Types

MapReduce accepts any iterable as input - arrays, generators, iterators, or custom iterables.

> **💡 True Streaming:**  Large files, database cursors, and API paginations are consumed as they're produced - never loading the entire dataset into memory.

### Arrays and Basic Iterables

```php
// Simple array
->input(['a', 'b', 'c'])

// Associative array
->input(['doc1' => 'text', 'doc2' => 'more text'])

// ArrayIterator
->input(new ArrayIterator($data))
```

### Files and Streams

```php
// Read file line by line (memory-efficient)
->input(new SplFileObject('large-file.txt'))

// Generator from file
->input((function() {
    $handle = fopen('data.csv', 'r');
    while (($line = fgets($handle)) !== false) {
        yield $line;
    }
    fclose($handle);
})())
```

### CSV Files

```php
// Parse CSV rows
->input((function() {
    $file = fopen('data.csv', 'r');
    fgetcsv($file); // Skip header
    while (($row = fgetcsv($file)) !== false) {
        yield [
            'name' => $row[0],
            'value' => $row[1],
        ];
    }
    fclose($file);
})())
```

### JSON Lines (JSONL)

```php
// Process newline-delimited JSON
->input((function() {
    $file = fopen('data.jsonl', 'r');
    while (($line = fgets($file)) !== false) {
        yield json_decode($line, true);
    }
    fclose($file);
})())
```

### Database Results

```php
// PDO result set
$stmt = $pdo->query('SELECT * FROM large_table');
->input($stmt)

// Generator for memory efficiency
->input((function() use ($pdo) {
    $stmt = $pdo->prepare('SELECT * FROM users');
    $stmt->execute();
    while ($row = $stmt->fetch()) {
        yield $row;
    }
})())
```

### API/HTTP Streams

```php
// Process paginated API results
->input((function() {
    $page = 1;
    do {
        $response = file_get_contents("https://api.example.com/data?page=$page");
        $data = json_decode($response, true);
        foreach ($data['items'] as $item) {
            yield $item;
        }
        $page++;
    } while (!empty($data['items']));
})())
```

### Directory Scanning

```php
// Process all files in a directory
->input(new RecursiveIteratorIterator(
    new RecursiveDirectoryIterator('/path/to/logs')
))

// Custom file filter
->input((function() {
    $dir = new RecursiveDirectoryIterator('/path/to/data');
    $iterator = new RecursiveIteratorIterator($dir);
    foreach ($iterator as $file) {
        if ($file->isFile() && $file->getExtension() === 'log') {
            yield $file->getPathname() => file_get_contents($file);
        }
    }
})())
```

## API Reference

### Core Methods (Required)

#### `input(iterable $input)`
Set the input data - any array or iterable.

```php
->input($documents)
->input(new ArrayIterator($data))
```

#### `map(callable $mapper)`
Define the transformation function. Must yield `[$key, $value]` pairs.

```php
->map(function ($key, $value) {
    // Process and emit key-value pairs
    yield [$newKey, $newValue];
})
```

#### `reduce(callable $reducer)`
Define the aggregation function. Receives all values for each key.

```php
->reduce(function ($key, array $values) {
    return array_sum($values);
})
```

#### `execute(): array`
Run the job and return results as `['key' => ['key' => $k, 'value' => $v], ...]`

### Configuration Methods (Optional)

#### `concurrent(int $workers)`
Number of parallel workers. Default: `4`

```php
->concurrent(8)  // Use 8 CPU cores
```

#### `partitions(int $count)`
Number of reduce partitions. Default: same as `concurrent()`

```php
->partitions(16)  // More parallelism in reduce phase
```

**Tip:** Use 1-2x concurrency level. More partitions = better parallelism but more overhead.

#### `chunkSize(int $records)`
Records per chunk during shuffle phase. Default: `10000`

Controls memory usage during sorting. Larger = faster but more RAM.

```php
->chunkSize(50000)   // High-memory system
->chunkSize(2000)    // Memory-constrained
```

#### `bufferSize(int $records)`
Records buffered before disk write. Default: `1000`

Controls I/O performance. Larger buffers = fewer disk writes but more RAM.

**Memory per partition:** `bufferSize × average_record_size` bytes

```php
->bufferSize(5000)   // Large datasets, plenty of RAM
->bufferSize(500)    // Memory-constrained
```

**Guidelines:**
- Small datasets (<10K): 100-500
- Medium datasets (10K-1M): 1000-5000
- Large datasets (>1M): 5000-10000

#### `workingDirectory(string $path)`
Directory for temporary files. Default: system temp

```php
->workingDirectory('/mnt/fast-ssd/tmp')
```

**Tip:** Use SSD storage for better performance on large datasets.

#### `partitionBy(callable $partitioner)`
Custom function to control which partition a key goes to. Must return `0` to `partitions-1`.

```php
->partitionBy(function ($key, $numPartitions) {
    return ord($key[0]) % $numPartitions;  // Partition by first letter
})
```

## Examples

### Aggregating Sales Data

```php
$sales = [
    ['product' => 'Widget', 'amount' => 100],
    ['product' => 'Gadget', 'amount' => 150],
    ['product' => 'Widget', 'amount' => 200],
];

$totals = (new MapReduceBuilder())
    ->input($sales)
    ->map(fn($id, $sale) => yield [$sale['product'], $sale['amount']])
    ->reduce(fn($product, $amounts) => [
        'total' => array_sum($amounts),
        'average' => array_sum($amounts) / count($amounts),
        'count' => count($amounts),
    ])
    ->execute();
```

### Building an Inverted Index

```php
$documents = [
    1 => 'the quick brown fox',
    2 => 'the lazy dog',
    3 => 'quick brown animals',
];

$invertedIndex = (new MapReduceBuilder())
    ->input($documents)
    ->map(function ($docId, $content) {
        foreach (array_unique(str_word_count(strtolower($content), 1)) as $word) {
            yield [$word, $docId];
        }
    })
    ->reduce(fn($word, $docIds) => [
        'documents' => array_unique($docIds),
        'frequency' => count($docIds),
    ])
    ->execute();
```

### Log Analysis

```php
$stats = (new MapReduceBuilder())
    ->input(file('access.log'))
    ->map(function ($lineNum, $line) {
        if (preg_match('/^(\S+).*?"GET (\S+).*?" (\d+)/', $line, $m)) {
            yield ["ip:{$m[1]}", 1];
            yield ["status:{$m[3]}", 1];
        }
    })
    ->reduce(fn($key, $counts) => array_sum($counts))
    ->concurrent(8)
    ->execute();
```

## Performance Tuning

### Quick Reference

```php
(new MapReduceBuilder())
    ->concurrent(8)              // CPU cores (or 2-3x for I/O tasks)
    ->partitions(16)             // 1-2x concurrency
    ->chunkSize(50000)           // Larger = faster, more RAM
    ->bufferSize(5000)           // Larger = fewer writes, more RAM
    ->workingDirectory('/ssd')   // Use fast storage for large jobs
```

### Memory vs Performance

**Low Memory System:**
```php
->chunkSize(2000)
->bufferSize(500)
```

**High Performance System (SSD, plenty RAM):**
```php
->chunkSize(100000)
->bufferSize(10000)
->workingDirectory('/mnt/fast-ssd/tmp')
```

### Benchmarks

Tested on 4-core, 16GB RAM, SSD:

| Records | Concurrency | Time | Memory |
|---------|-------------|------|--------|
| 10K     | 4           | 0.5s | 80MB   |
| 100K    | 4           | 3s   | 150MB  |
| 1M      | 8           | 25s  | 280MB  |
| 10M     | 8           | 4m30s| 420MB  |

## Testing

Run tests:
```bash
composer test           # Run test suite
composer test-coverage  # With coverage report
composer analyse        # Static analysis (PHPStan)
composer cs-check       # Code style check (PSR-12)
```

## Limitations

- Single machine only (not a distributed cluster)
- Requires disk space for intermediate files
- Process spawning overhead makes it inefficient for tiny datasets (<1000 records)

## License

MIT License - see [LICENSE](LICENSE) file

## Support

- Issues: [github.com/spiritinlife/php-mapreduce/issues](https://github.com/spiritinlife/php-mapreduce/issues)
- Email: george.chailazopoulos@gmail.com