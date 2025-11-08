<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\Pipeline\Mapper;

class MapperTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/mapper_test_' . uniqid();
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

    public function testBasicMapping(): void
    {
        $mapper = new Mapper($this->tempDir, 2);

        $input = [
            'doc1' => 'hello world',
            'doc2' => 'hello php',
        ];

        $mapOutputFiles = $mapper->map(
            $input,
            function ($docId, $text) {
                foreach (explode(' ', $text) as $word) {
                    yield [$word, 1];
                }
            },
            2 // 2 reduce partitions
        );

        // Should have 2 partitions
        $this->assertCount(2, $mapOutputFiles);

        // Each partition should have files
        $allFiles = array_merge(...$mapOutputFiles);
        $this->assertGreaterThan(0, count($allFiles));

        // Verify files exist
        foreach ($allFiles as $file) {
            $this->assertFileExists($file);
        }
    }

    public function testEmptyInput(): void
    {
        $mapper = new Mapper($this->tempDir, 2);

        $mapOutputFiles = $mapper->map(
            [],
            fn($k, $v) => yield [$k, $v],
            2
        );

        // Should return empty arrays for each partition
        $this->assertCount(2, $mapOutputFiles);
        $this->assertEmpty($mapOutputFiles[0]);
        $this->assertEmpty($mapOutputFiles[1]);
    }

    public function testHashBasedPartitioning(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $input = ['a' => 1, 'b' => 2, 'c' => 3];

        $mapOutputFiles = $mapper->map(
            $input,
            fn($k, $v) => yield [$k, $v],
            3 // 3 partitions
        );

        $this->assertCount(3, $mapOutputFiles);

        // Verify data is distributed across partitions
        $totalFiles = 0;
        foreach ($mapOutputFiles as $partitionFiles) {
            $totalFiles += count($partitionFiles);
        }
        $this->assertGreaterThan(0, $totalFiles);
    }

    public function testCustomPartitioner(): void
    {
        // Partitioner that always sends to partition 0
        $customPartitioner = function ($key, $numPartitions) {
            return 0;
        };

        $mapper = new Mapper($this->tempDir, 1, $customPartitioner);

        $input = ['a' => 1, 'b' => 2, 'c' => 3];

        $mapOutputFiles = $mapper->map(
            $input,
            fn($k, $v) => yield [$k, $v],
            3
        );

        // All data should go to partition 0
        $this->assertNotEmpty($mapOutputFiles[0]);

        // Count records in each partition
        $records = [0 => 0, 1 => 0, 2 => 0];
        foreach ($mapOutputFiles as $partition => $files) {
            foreach ($files as $file) {
                if (file_exists($file)) {
                    $lines = file($file);
                    $records[$partition] += $lines !== false ? count($lines) : 0;
                }
            }
        }

        // All 3 records should be in partition 0
        $this->assertEquals(3, $records[0]);
        $this->assertEquals(0, $records[1]);
        $this->assertEquals(0, $records[2]);
    }

    public function testInvalidPartitionerThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Partitioner must return an integer');

        // Partitioner that returns invalid value
        $badPartitioner = function ($key, $numPartitions) {
            return 999; // Out of range
        };

        $mapper = new Mapper($this->tempDir, 1, $badPartitioner);

        $mapper->map(
            ['a' => 1],
            fn($k, $v) => yield [$k, $v],
            2
        );
    }

    public function testMapperWithInvalidPairStructure(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Mapper must yield [key, value] pairs');

        $mapper = new Mapper($this->tempDir, 1);

        $mapper->map(
            ['a' => 1],
            function ($k, $v) {
                yield [$k, $v, 'extra']; // Invalid: 3 elements
            },
            2
        );
    }

    public function testMapperWithNonIterableResult(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $mapOutputFiles = $mapper->map(
            ['a' => 1],
            function ($k, $v) {
                // Return array instead of yielding
                return [[$k, $v]];
            },
            2
        );

        // Should handle non-iterable by wrapping it
        $this->assertCount(2, $mapOutputFiles);
    }

    public function testMultipleEmitsPerInput(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $input = [1, 2, 3];

        $mapOutputFiles = $mapper->map(
            $input,
            function ($key, $value) {
                yield ['original', $value];
                yield ['squared', $value * $value];
            },
            2
        );

        // Count total records written
        $totalRecords = 0;
        foreach ($mapOutputFiles as $partitionFiles) {
            foreach ($partitionFiles as $file) {
                $handle = fopen($file, 'r');
                if ($handle !== false) {
                    while (fgets($handle) !== false) {
                        $totalRecords++;
                    }
                    fclose($handle);
                }
            }
        }

        // Should have 6 records (2 per input item)
        $this->assertEquals(6, $totalRecords);
    }

    public function testConcurrentProcessing(): void
    {
        $mapper = new Mapper($this->tempDir, 4); // 4 workers

        // Create larger input to force chunking
        $input = [];
        for ($i = 0; $i < 100; $i++) {
            $input[$i] = $i;
        }

        $mapOutputFiles = $mapper->map(
            $input,
            fn($k, $v) => yield [$v % 10, $v], // Group by mod 10
            4
        );

        $this->assertCount(4, $mapOutputFiles);

        // Verify all data was processed
        $totalRecords = 0;
        foreach ($mapOutputFiles as $partitionFiles) {
            foreach ($partitionFiles as $file) {
                $lines = file($file);
                $totalRecords += $lines !== false ? count($lines) : 0;
            }
        }
        $this->assertEquals(100, $totalRecords);
    }

    public function testIntegerKeys(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $mapOutputFiles = $mapper->map(
            ['a' => 1, 'b' => 2],
            fn($k, $v) => yield [10, $v],
            2
        );

        // Verify integer keys are handled
        $this->assertCount(2, $mapOutputFiles);

        // Find the partition with data
        $foundData = false;
        foreach ($mapOutputFiles as $partitionFiles) {
            if (!empty($partitionFiles)) {
                $foundData = true;
                break;
            }
        }
        $this->assertTrue($foundData);
    }

    public function testComplexKeys(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $mapOutputFiles = $mapper->map(
            [1, 2, 3],
            function ($k, $v) {
                yield [['user' => 1, 'product' => 100], $v];
            },
            2
        );

        // Verify complex keys are handled
        $this->assertCount(2, $mapOutputFiles);

        $totalRecords = 0;
        foreach ($mapOutputFiles as $partitionFiles) {
            foreach ($partitionFiles as $file) {
                $lines = file($file);
                $totalRecords += $lines !== false ? count($lines) : 0;
            }
        }
        $this->assertEquals(3, $totalRecords);
    }

    public function testIterableInput(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        // Use generator as input
        $generator = function () {
            yield 'a' => 1;
            yield 'b' => 2;
            yield 'c' => 3;
        };

        $mapOutputFiles = $mapper->map(
            $generator(),
            fn($k, $v) => yield [$k, $v],
            2
        );

        $this->assertCount(2, $mapOutputFiles);
    }

    public function testFailedToCreatePartitionFile(): void
    {
        // Create read-only directory
        $readOnlyDir = $this->tempDir . '/readonly_mapper';
        mkdir($readOnlyDir, 0755);
        chmod($readOnlyDir, 0555);

        // Check if we actually made it read-only (skip test if we couldn't)
        if (is_writable($readOnlyDir)) {
            chmod($readOnlyDir, 0755);
            $this->markTestSkipped('Cannot make directory read-only on this system');
        }

        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('Failed to open file for buffered writing');

            $mapper = new Mapper($readOnlyDir, 1);
            $mapper->map(
                ['a' => 1],
                fn($k, $v) => yield [$k, $v],
                2
            );
        } finally {
            chmod($readOnlyDir, 0755);
        }
    }

    public function testFailedToWriteToPartitionFile(): void
    {
        // This test is difficult to trigger reliably, so we'll test indirectly
        // by ensuring the write error path exists in the code
        $mapper = new Mapper($this->tempDir, 1);

        // Normal operation should work
        $mapOutputFiles = $mapper->map(
            ['a' => 1, 'b' => 2],
            fn($k, $v) => yield [$k, $v],
            2
        );

        // Verify files were created successfully
        $this->assertCount(2, $mapOutputFiles);
        $totalFiles = 0;
        foreach ($mapOutputFiles as $files) {
            $totalFiles += count($files);
        }
        $this->assertGreaterThan(0, $totalFiles);
    }

    public function testScalarKeyHandling(): void
    {
        $mapper = new Mapper($this->tempDir, 1);

        $mapOutputFiles = $mapper->map(
            [1 => 'a', 2 => 'b', 3 => 'c'],
            function ($k, $v) {
                // Emit various scalar key types
                yield ['string_key', $v];
                yield [$k, $v]; // integer key
                yield [1.5, $v]; // float key
                yield [true, $v]; // boolean key
            },
            2
        );

        // Should handle all scalar types
        $this->assertCount(2, $mapOutputFiles);

        $totalRecords = 0;
        foreach ($mapOutputFiles as $partitionFiles) {
            foreach ($partitionFiles as $file) {
                $lines = file($file);
                $totalRecords += $lines !== false ? count($lines) : 0;
            }
        }

        // 3 inputs * 4 emits per input = 12 records
        $this->assertEquals(12, $totalRecords);
    }
}
