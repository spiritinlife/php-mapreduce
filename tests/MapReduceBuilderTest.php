<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\MapReduceBuilder;

class MapReduceBuilderTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/builder_test_' . uniqid();
        mkdir($this->tempDir, 0755, true);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        if (is_dir($this->tempDir)) {
            $this->recursiveDelete($this->tempDir);
        }
    }

    private function recursiveDelete(string $dir): void
    {
        $files = scandir($dir);
        if ($files !== false) {
            $files = array_diff($files, ['.', '..']);
            foreach ($files as $file) {
                $path = "$dir/$file";
                is_dir($path) ? $this->recursiveDelete($path) : unlink($path);
            }
        }
        rmdir($dir);
    }

    public function testFluentInterface(): void
    {
        $builder = new MapReduceBuilder();

        // All methods should return the builder instance for chaining
        $this->assertSame($builder, $builder->input([1, 2, 3]));
        $this->assertSame($builder, $builder->map(fn($k, $v) => yield [$k, $v]));
        $this->assertSame($builder, $builder->reduce(fn($k, $v) => $v));
        $this->assertSame($builder, $builder->concurrent(4));
        $this->assertSame($builder, $builder->partitions(8));
        $this->assertSame($builder, $builder->chunkSize(1000));
        $this->assertSame($builder, $builder->workingDirectory($this->tempDir));
        $this->assertSame($builder, $builder->partitionBy(fn($k, $n) => 0));
    }

    public function testInputConfiguration(): void
    {
        $result = (new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v[0])
            ->execute();

        $this->assertCount(2, $result);
        $this->assertEquals(1, $result['a']['value']);
        $this->assertEquals(2, $result['b']['value']);
    }

    public function testConcurrentConfiguration(): void
    {
        $result = (new MapReduceBuilder())
            ->input(range(1, 10))
            ->map(fn($k, $v) => yield ['sum', $v])
            ->reduce(fn($k, $v) => array_sum($v))
            ->concurrent(2) // 2 workers
            ->execute();

        $this->assertEquals(55, $result['sum']['value']); // Sum of 1..10
    }

    public function testPartitionsConfiguration(): void
    {
        $result = (new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2, 'c' => 3])
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v[0])
            ->partitions(4) // 4 reduce partitions
            ->execute();

        $this->assertCount(3, $result);
    }

    public function testChunkSizeConfiguration(): void
    {
        $result = (new MapReduceBuilder())
            ->input(range(1, 100))
            ->map(fn($k, $v) => yield ['total', $v])
            ->reduce(fn($k, $v) => array_sum($v))
            ->chunkSize(10) // Small chunks
            ->execute();

        $this->assertEquals(5050, $result['total']['value']); // Sum of 1..100
    }

    public function testWorkingDirectoryConfiguration(): void
    {
        $customDir = $this->tempDir . '/custom_work';

        $result = (new MapReduceBuilder())
            ->input(['x' => 1])
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v[0])
            ->workingDirectory($customDir)
            ->execute();

        $this->assertEquals(1, $result['x']['value']);
        // Directory should be cleaned up after execution
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testPartitionByConfiguration(): void
    {
        // Custom partitioner that groups by first letter
        $result = (new MapReduceBuilder())
            ->input(['apple', 'apricot', 'banana', 'blueberry'])
            ->map(fn($k, $word) => yield [$word, strlen($word)])
            ->partitionBy(function ($word, $numPartitions) {
                return ord(substr($word, 0, 1)) % $numPartitions;
            })
            ->reduce(fn($word, $lengths) => $lengths[0])
            ->partitions(3)
            ->execute();

        $this->assertEquals(5, $result['apple']['value']);
        $this->assertEquals(6, $result['banana']['value']);
    }

    public function testMissingInputThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Input data is required');

        (new MapReduceBuilder())
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v)
            ->execute();
    }

    public function testMissingMapperThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Mapper function is required');

        (new MapReduceBuilder())
            ->input([1, 2, 3])
            ->reduce(fn($k, $v) => $v)
            ->execute();
    }

    public function testMissingReducerThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Reducer function is required');

        (new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(fn($k, $v) => yield [$k, $v])
            ->execute();
    }

    public function testInvalidConcurrencyThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Concurrency must be at least 1');

        (new MapReduceBuilder())
            ->concurrent(0);
    }

    public function testInvalidPartitionsThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Partitions must be at least 1');

        (new MapReduceBuilder())
            ->partitions(0);
    }

    public function testInvalidChunkSizeThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk size must be at least 1');

        (new MapReduceBuilder())
            ->chunkSize(0);
    }

    public function testCompleteWorkflow(): void
    {
        // Test a complete workflow with all configuration options
        $result = (new MapReduceBuilder())
            ->input([
                'doc1' => 'the quick brown fox',
                'doc2' => 'the lazy dog',
                'doc3' => 'quick brown dogs',
            ])
            ->map(function ($docId, $text) {
                $words = str_word_count(strtolower($text), 1);
                foreach ($words as $word) {
                    yield [$word, 1];
                }
            })
            ->reduce(function ($word, $counts) {
                return array_sum($counts);
            })
            ->concurrent(2)
            ->partitions(4)
            ->chunkSize(100)
            ->workingDirectory($this->tempDir . '/complete')
            ->execute();

        // Verify word counts
        $this->assertEquals(2, $result['the']['value']);
        $this->assertEquals(2, $result['quick']['value']);
        $this->assertEquals(2, $result['brown']['value']);
        $this->assertEquals(1, $result['fox']['value']);
        $this->assertEquals(1, $result['lazy']['value']);
        $this->assertEquals(1, $result['dog']['value']);
        $this->assertEquals(1, $result['dogs']['value']);
    }

    public function testDefaultConfiguration(): void
    {
        // Test with minimal configuration (using defaults)
        $result = (new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v[0])
            ->execute();

        $this->assertCount(2, $result);
    }

    public function testIterableInput(): void
    {
        // Test with generator input
        $generator = function () {
            yield 'x' => 10;
            yield 'y' => 20;
            yield 'z' => 30;
        };

        $result = (new MapReduceBuilder())
            ->input($generator())
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v[0])
            ->execute();

        $this->assertCount(3, $result);
        $this->assertEquals(10, $result['x']['value']);
        $this->assertEquals(20, $result['y']['value']);
        $this->assertEquals(30, $result['z']['value']);
    }

    public function testEmptyInput(): void
    {
        $result = (new MapReduceBuilder())
            ->input([])
            ->map(fn($k, $v) => yield [$k, $v])
            ->reduce(fn($k, $v) => $v)
            ->execute();

        $this->assertEmpty($result);
    }

    public function testMethodChainingOrder(): void
    {
        // Test that methods can be called in any order
        $result = (new MapReduceBuilder())
            ->concurrent(2)
            ->partitions(4)
            ->input(['a' => 1])
            ->chunkSize(100)
            ->reduce(fn($k, $v) => $v[0])
            ->map(fn($k, $v) => yield [$k, $v])
            ->execute();

        $this->assertEquals(1, $result['a']['value']);
    }
}
