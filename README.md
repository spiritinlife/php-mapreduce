# PHP MapReduce

A high-performance, framework-agnostic MapReduce implementation for PHP that processes large datasets using **true parallel processing** with separate PHP worker processes and disk-based storage.

[![PHP Version](https://img.shields.io/badge/php-%5E8.1-blue)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Why Use This?

- **True CPU parallelism** - spawns separate PHP processes for parallel execution, not async coroutines
- Process datasets larger than RAM using memory-efficient disk storage
- **Stream iterators without loading into memory** - process large files, database cursors, and API responses efficiently
- Utilize multiple CPU cores for CPU-intensive transformations
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
    ->map(function ($text, $context) {
        foreach (str_word_count(strtolower($text), 1) as $word) {
            yield [$word, 1];
        }
    })
    ->reduce(function ($word, $countsIterator, $context) {
        $sum = 0;
        foreach ($countsIterator as $count) {
            $sum += $count;
        }
        return $sum;
    })
    ->execute();

// Results are streamed as a generator - iterate to process results
foreach ($results as $word => $count) {
    // Output: 'quick' => 2, 'brown' => 2, 'the' => 2, ...
}
```

## How It Works

MapReduce processes data in three parallel phases using separate PHP worker processes:

1. **Map Phase** - Input is streamed in batches to parallel worker processes. Each worker emits key-value pairs to partitioned temporary files.
2. **Shuffle Phase** - Partitions are sorted and grouped in parallel using memory-efficient external sorting with separate worker processes.
3. **Reduce Phase** - Partitions are processed in parallel by worker processes, aggregating values by key.

```
Input → [Parallel Map Workers] → [Parallel Shuffle & Sort] → [Parallel Reduce Workers] → Results
```

Each phase uses `spatie/async` to spawn separate PHP processes, enabling true CPU parallelism for compute-intensive operations.

> **⚠️ Important:** Because mapper and reducer functions run in separate processes, you **cannot use closure `use` clauses** to access external variables. Use the `context()` method instead. See [Parallel Processing & Context](#parallel-processing--context) for details.

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
Define the transformation function. Receives `($value, $context)` and must yield `[$key, $value]` pairs.

```php
->map(function ($value, $context) {
    // Process and emit key-value pairs
    yield [$newKey, $newValue];
})
```

#### `reduce(callable $reducer)`
Define the aggregation function using an **accumulator + iterator pattern**.

The reducer receives `($key, $valuesIterator, $context)` where:
- `$key` - The current key being reduced
- `$valuesIterator` - Iterator that streams values one at a time for memory efficiency
- `$context` - Optional shared context data (see [context()](#contextmixed-data) method)

You manually accumulate results by iterating over the values. This approach never loads all values for a key into memory at once, which is critical for keys with many values (e.g., a global counter with millions of entries).

```php
// Accumulator pattern - iterate and accumulate manually
->reduce(function ($key, $valuesIterator, $context) {
    $sum = 0;  // Accumulator
    foreach ($valuesIterator as $value) {
        $sum += $value;  // Accumulate
    }
    return $sum;
})

// Using context data
->reduce(function ($key, $valuesIterator, $context) {
    $threshold = $context['threshold'];
    $sum = 0;
    foreach ($valuesIterator as $value) {
        if ($value > $threshold) {
            $sum += $value;
        }
    }
    return $sum;
})

// If you need the full array (uses memory):
->reduce(function ($key, $valuesIterator, $context) {
    $values = iterator_to_array($valuesIterator);
    return ['count' => count($values), 'unique' => array_unique($values)];
})
```

#### `context(mixed $data)`
Pass shared data to mapper and reducer functions executing in parallel processes.

Because mapper and reducer functions run in **separate PHP processes**, you cannot use closure `use` clauses to access external variables. Use `context()` instead to pass serializable data that will be available to all workers.

```php
// ❌ This doesn't work - variables won't be available in parallel processes
$threshold = 100;
->map(function ($value, $context) use ($threshold) {  // Won't work!
    if ($value > $threshold) yield [$value, 1];
})

// ✅ Use context() instead
->context(['threshold' => 100])
->map(function ($value, $context) {
    if ($value > $context['threshold']) yield [$value, 1];
})
```

The context is passed as an additional parameter to both mapper and reducer:
- **Mapper:** `function($value, $context)`
- **Reducer:** `function($key, $valuesIterator, $context)`

See [Parallel Processing & Context](#parallel-processing--context) for details.

#### `execute(): Generator`
Run the job and return results as a generator

### Configuration Methods (Optional)

#### `concurrent(int $workers)`
Number of parallel worker processes. Default: `4`

```php
->concurrent(8)  // Spawn 8 parallel PHP processes
```

**Tip:** Match your CPU core count. For I/O-bound tasks, you can use 2-3x cores.

#### `partitions(int $count)`
Number of reduce partitions for parallel processing. Default: same as `concurrent()`

```php
->partitions(16)  // More parallelism in reduce phase
```

**Tip:** Use 1-2x concurrency level. More partitions = better parallelism but more overhead.

#### `mapperBatchSize(int $items)`
**Controls:** Items sent to each parallel worker process. Default: `500`

Determines how many input items are batched together and sent to a worker process. This is **critical for memory management** because it affects how many tasks the parent process creates.

```php
->mapperBatchSize(2000)   // Large datasets - reduce process overhead
->mapperBatchSize(100)    // Small datasets - better load balancing
```

**Tradeoffs:**
- **Smaller batches** = Better load balancing, **MORE tasks = MORE parent memory**
- **Larger batches** = Less overhead, fewer tasks, **LESS parent memory**

**Guidelines:**
- Small datasets (<10K records): 100-500 (default)
- Medium datasets (10K-100K): 500-2,000
- Large datasets (100K-1M): 2,000-5,000
- Very large datasets (>1M): 5,000-10,000

> **⚠️ Critical: Batch Size vs Memory**
>
> Parent process memory is proportional to the **number of tasks**, not dataset size:
> - 1M records ÷ 500 batch = **2,000 tasks** (may use 100MB+ parent memory)
> - 1M records ÷ 5,000 batch = **200 tasks** (uses ~10MB parent memory)
>
> **For large datasets (>100K records), use larger batch sizes to reduce parent process memory overhead.**

#### `shuffleChunkSize(int $records)`
**Controls:** Memory usage during shuffle phase external sorting. Default: `10000`

Determines how many records accumulate in memory before being sorted and written as a chunk file during external sorting. Only affects the shuffle phase.

```php
->shuffleChunkSize(50000)   // High-memory system - faster sorting
->shuffleChunkSize(2000)    // Memory-constrained
```

**Memory impact:** `shuffleChunkSize × average_record_size` bytes per sort operation

**Guidelines:**
- Small datasets (<100K): 5,000-10,000
- Medium datasets (100K-1M): 10,000-50,000 (default: 10,000)
- Large datasets (>1M): 50,000-100,000

#### `bufferSize(int $records)`
**Controls:** File I/O write buffering across all phases. Default: `1000`

Determines how many records are buffered in memory before flushing to disk. Affects the frequency of `fwrite()` system calls. Used by map, shuffle, and reduce phases.

```php
->bufferSize(5000)   // Large datasets, plenty of RAM
->bufferSize(500)    // Memory-constrained
```

**Memory impact:** `bufferSize × average_record_size × num_partitions` bytes in map phase (each partition has its own buffer)

**Guidelines:**
- Small datasets (<10K): 100-500
- Medium datasets (10K-1M): 1000-5000 (default: 1000)
- Large datasets (>1M): 5000-10000

> **💡 Three Independent Parameters:**
>
> - **`mapperBatchSize`** - Controls worker granularity & **parent memory** (map phase only)
> - **`shuffleChunkSize`** - Controls sorting memory (shuffle phase only)
> - **`bufferSize`** - Controls write batching (all phases)
>
> These handle different concerns:
> - Large `mapperBatchSize` reduces process spawning overhead **AND parent memory** (critical for >100K records)
> - Large `shuffleChunkSize` creates fewer chunk files to merge
> - Large `bufferSize` reduces system call overhead
>
> **Most impactful for large datasets:** Increase `mapperBatchSize` first!

#### `workingDirectory(string $path)`
Directory for temporary files. Default: system temp

```php
->workingDirectory('/mnt/fast-ssd/tmp')
```

**Tip:** Use SSD storage for better performance on large datasets.

#### `autoload(string $path)`
Configure autoloader path for parallel worker processes. Default: none

When using parallel processing, worker processes spawn in separate contexts and may need to load dependencies. This method specifies the path to your autoloader (typically `vendor/autoload.php`).

```php
->autoload(__DIR__ . '/vendor/autoload.php')
```

**When to use:**
- When mapper/reducer functions use classes from external libraries
- When using custom classes that need autoloading
- If you get "class not found" errors in worker processes

**Note:** Most of the time you won't need this - the library handles common cases automatically. Only use if you encounter class loading issues in parallel workers.

#### `partitionBy(callable $partitioner)`
Custom function to control which partition a key goes to. Receives `($key, $numPartitions, $context)` and must return `0` to `partitions-1`.

```php
->partitionBy(function ($key, $numPartitions, $context) {
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
    ->map(fn($sale, $context) => yield [$sale['product'], $sale['amount']])
    ->reduce(function($product, $amountsIterator, $context) {
        // Collect amounts to calculate statistics
        $amounts = iterator_to_array($amountsIterator);
        return [
            'total' => array_sum($amounts),
            'average' => array_sum($amounts) / count($amounts),
            'count' => count($amounts),
        ];
    })
    ->execute();

foreach ($totals as $product => $stats) {
    echo "{$product}: {$stats['total']}\n";
}
```

### Building an Inverted Index

```php
$documents = [
    ['id' => 1, 'text' => 'the quick brown fox'],
    ['id' => 2, 'text' => 'the lazy dog'],
    ['id' => 3, 'text' => 'quick brown animals'],
];

$invertedIndex = (new MapReduceBuilder())
    ->input($documents)
    ->map(function ($doc, $context) {
        foreach (array_unique(str_word_count(strtolower($doc['text']), 1)) as $word) {
            yield [$word, $doc['id']];
        }
    })
    ->reduce(function($word, $docIdsIterator, $context) {
        // Collect document IDs to calculate frequency and uniqueness
        $docIds = iterator_to_array($docIdsIterator);
        return [
            'documents' => array_unique($docIds),
            'frequency' => count($docIds),
        ];
    })
    ->execute();

foreach ($invertedIndex as $word => $index) {
    echo "{$word}: appears in {$index['frequency']} documents\n";
}
```

### Log Analysis

```php
$stats = (new MapReduceBuilder())
    ->input(file('access.log'))
    ->map(function ($line, $context) {
        if (preg_match('/^(\S+).*?"GET (\S+).*?" (\d+)/', $line, $m)) {
            yield ["ip:{$m[1]}", 1];
            yield ["status:{$m[3]}", 1];
        }
    })
    ->reduce(function($key, $countsIterator, $context) {
        $sum = 0;
        foreach ($countsIterator as $count) {
            $sum += $count;
        }
        return $sum;
    })
    ->concurrent(8)
    ->execute();

foreach ($stats as $key => $count) {
    echo "{$key}: {$count}\n";
}
```

### Using Context for Shared Data

```php
// Pre-calculated lookup tables or configuration
$userTiers = ['alice' => 'gold', 'bob' => 'silver', 'charlie' => 'bronze'];
$tierMultipliers = ['gold' => 1.5, 'silver' => 1.2, 'bronze' => 1.0];

$purchases = [
    ['user' => 'alice', 'amount' => 100],
    ['user' => 'bob', 'amount' => 150],
    ['user' => 'charlie', 'amount' => 200],
];

$results = (new MapReduceBuilder())
    ->input($purchases)
    // Pass shared data via context (not via 'use' clause!)
    ->context([
        'userTiers' => $userTiers,
        'tierMultipliers' => $tierMultipliers,
    ])
    ->map(function ($purchase, $context) {
        // Access context data in mapper
        $tier = $context['userTiers'][$purchase['user']] ?? 'bronze';
        $multiplier = $context['tierMultipliers'][$tier];
        $bonusPoints = $purchase['amount'] * $multiplier;

        yield [$purchase['user'], $bonusPoints];
    })
    ->reduce(function ($user, $pointsIterator, $context) {
        // Context also available in reducer
        $tier = $context['userTiers'][$user];

        // Stream and sum points without loading all into memory
        $totalPoints = 0;
        foreach ($pointsIterator as $points) {
            $totalPoints += $points;
        }

        return [
            'total_points' => $totalPoints,
            'tier' => $tier,
        ];
    })
    ->execute();

foreach ($results as $user => $data) {
    echo "{$user} ({$data['tier']}): {$data['total_points']} points\n";
}
```

## Parallel Processing & Context

### Important: Closure `use` Clauses Don't Work

Because MapReduce uses **true parallel processing with separate PHP processes** (via `spatie/async`), variables captured with `use` clauses are **NOT available** in mapper and reducer functions.

```php
// ❌ BROKEN - Variables won't be available in parallel processes
$totalUsers = 1000;
$lookupTable = ['a' => 1, 'b' => 2];

(new MapReduceBuilder())
    ->input($data)
    ->map(function ($item, $context) use ($lookupTable) {
        // ❌ $lookupTable will be NULL or empty here!
        $value = $lookupTable[$item['key']];  // Won't work!
        yield [$item['key'], $value];
    })
    ->reduce(function ($key, $valuesIterator, $context) use ($totalUsers) {
        // ❌ $totalUsers will be NULL or 0 here!
        $sum = 0;
        foreach ($valuesIterator as $value) {
            $sum += $value;
        }
        $percentage = $sum / $totalUsers;  // Won't work!
        return $percentage;
    })
    ->execute();
```

### Solution: Use `context()` Method

Pass data via the `context()` method, which serializes and provides it to all workers:

```php
// ✅ CORRECT - Use context() to pass data
$totalUsers = 1000;
$lookupTable = ['a' => 1, 'b' => 2];

(new MapReduceBuilder())
    ->input($data)
    ->context([
        'totalUsers' => $totalUsers,
        'lookupTable' => $lookupTable,
        'config' => ['threshold' => 50]
    ])
    ->map(function ($item, $context) {
        // ✅ Access via $context parameter
        $value = $context['lookupTable'][$item['key']];
        yield [$item['key'], $value];
    })
    ->reduce(function ($key, $valuesIterator, $context) {
        // ✅ Context available in reducer too
        $sum = 0;
        foreach ($valuesIterator as $value) {
            $sum += $value;
        }
        $percentage = $sum / $context['totalUsers'];
        return $percentage;
    })
    ->execute();
```

### Context Guidelines

**What can be passed as context:**
- Arrays, scalars (int, float, string, bool)
- Objects (as long as they're serializable - no resources, DB connections, or file handles)
- Nested data structures
- Class constants and configuration values

**Best practices:**
- Keep context reasonably sized (< 100MB recommended) - it's serialized to each worker
- Context is read-only - changes in one worker won't affect others
- All data must be serializable (no resources, closures, or database connections)

**Real-world example (collaborative filtering):**

```php
// Phase 1: Calculate counts (stored in memory)
$totalBuyers = 10000;
$eventCounts = [
    'follow' => ['seller1' => 500, 'seller2' => 300],
    'purchase' => ['seller1' => 200, 'seller2' => 150],
];

// Phase 2: Use counts to calculate scores with MapReduce
$results = (new MapReduceBuilder())
    ->input($cooccurrences)
    ->context([
        'totalBuyers' => $totalBuyers,
        'eventCounts' => $eventCounts,
        'minScore' => 5.0
    ])
    ->reduce(function ($key, $valuesIterator, $context) {
        // Extract shared data from context
        $totalBuyers = $context['totalBuyers'];
        $eventCounts = $context['eventCounts'];

        // Collect values for calculation
        $values = iterator_to_array($valuesIterator);

        // Use in calculations
        $score = calculateScore($values, $totalBuyers, $eventCounts);

        if ($score >= $context['minScore']) {
            return ['score' => $score, 'count' => count($values)];
        }

        return null;
    })
    ->execute();
```

## Performance Tuning

### Quick Reference

```php
(new MapReduceBuilder())
    ->concurrent(8)                  // CPU cores (or 2-3x for I/O tasks)
    ->partitions(16)                 // 1-2x concurrency for reduce parallelism
    ->mapperBatchSize(2000)          // Items per worker batch (larger = less overhead)
    ->shuffleChunkSize(50000)        // Shuffle sort memory (larger = fewer merges)
    ->bufferSize(5000)               // Write buffer (larger = fewer syscalls)
    ->workingDirectory('/ssd')       // Use fast storage for large jobs
    ->autoload(__DIR__ . '/vendor/autoload.php')  // Optional: for custom class loading
```

### Memory vs Performance

**Low Memory System (2GB RAM):**
```php
->mapperBatchSize(200)       // Small batches to reduce serialization
->shuffleChunkSize(2000)     // Small sort chunks
->bufferSize(500)            // Small write buffers
```

**High Performance System (SSD, 32GB RAM):**
```php
->mapperBatchSize(5000)      // Large batches to reduce process overhead
->shuffleChunkSize(100000)   // Large sort chunks for fast sorting
->bufferSize(10000)          // Large buffers to minimize disk I/O
->workingDirectory('/mnt/fast-ssd/tmp')
```

**Tuning independently:**
- **Large dataset (>100K records)? CRITICAL: Increase `mapperBatchSize`** to drastically reduce parent process memory (can drop from 100MB+ to <10MB)
- Slow disk? Increase `bufferSize` to batch more writes
- Limited RAM during shuffle? Decrease `shuffleChunkSize` to reduce sort memory
- Many concurrent workers + partitions? Decrease `bufferSize` (memory = workers × partitions × bufferSize)
- Parent process using too much memory? Increase `mapperBatchSize` (fewer tasks = less overhead)

### Benchmarks

Tested on Apple M2 Pro (10-core), 16GB RAM, SSD with `spatie/async` parallel processing:

| Records | Concurrency | Time  | Parent Memory* | Batch Size |
|---------|-------------|-------|----------------|------------|
| 10K     | 4           | 0.3s  | 6MB            | 500        |
| 100K    | 4           | 1.2s  | 0MB            | 2,000      |
| 1M      | 8           | 7.9s  | 8MB            | 5,000      |
| 10M     | 8           | 1m23s | 10MB           | 5,000      |

*Parent process memory only - worker processes use separate memory space

**Key Insight:** Using larger `mapperBatchSize` for datasets >100K dramatically reduces parent memory (from 100MB+ to <10MB) by creating fewer tasks. See the critical callout in `mapperBatchSize()` documentation above.

Run your own benchmarks:
```bash
php bin/benchmark-readme.php
```

## Testing

Run tests:
```bash
composer test           # Run test suite
composer test-coverage  # With coverage report
composer analyse        # Static analysis (PHPStan)
composer cs-check       # Code style check (PSR-12)
```

## Limitations

- **Single machine only** (not a distributed cluster)
- **Requires disk space** for intermediate files
- **Process spawning overhead** makes it inefficient for tiny datasets (<1000 records)
- **Closure `use` clauses don't work** in mapper/reducer functions due to parallel processing - use `context()` method instead (see [Parallel Processing & Context](#parallel-processing--context))

## License

MIT License - see [LICENSE](LICENSE) file