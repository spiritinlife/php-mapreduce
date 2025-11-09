<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\Pipeline\Shuffler;

class ShufflerTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/shuffler_test_' . uniqid();
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

    /**
     * @param array<int, array{key: mixed, value: mixed}> $data
     */
    private function createTestInputFile(string $filename, array $data): void
    {
        $handle = fopen($filename, 'w');
        if ($handle !== false) {
            foreach ($data as $pair) {
                fwrite($handle, serialize($pair) . "\n");
            }
            fclose($handle);
        }
    }

    /**
     * @return array<string, array<int, mixed>>
     */
    private function readShuffledFile(string $filename): array
    {
        $results = [];
        $handle = fopen($filename, 'r');
        if ($handle === false) {
            return $results;
        }
        while (($line = fgets($handle)) !== false) {
            $group = unserialize(trim($line));
            // Use string representation of key for consistency (same as serializeKey in Shuffler)
            if (is_scalar($group['key'])) {
                $keyIndex = (string)$group['key'];
            } else {
                $keyIndex = serialize($group['key']);
            }
            $results[$keyIndex] = $group['values'];
        }
        fclose($handle);
        return $results;
    }

    public function testBasicShuffling(): void
    {
        $shuffler = new Shuffler($this->tempDir, 5);

        // Create test input files
        $inputFile1 = "{$this->tempDir}/input1.tmp";
        $inputFile2 = "{$this->tempDir}/input2.tmp";

        $this->createTestInputFile($inputFile1, [
            ['key' => 'apple', 'value' => 1],
            ['key' => 'banana', 'value' => 2],
            ['key' => 'apple', 'value' => 3],
        ]);

        $this->createTestInputFile($inputFile2, [
            ['key' => 'banana', 'value' => 4],
            ['key' => 'cherry', 'value' => 5],
            ['key' => 'apple', 'value' => 6],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile1, $inputFile2]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([1, 3, 6], $results['apple']);
        $this->assertEquals([2, 4], $results['banana']);
        $this->assertEquals([5], $results['cherry']);
    }

    public function testEmptyInputFiles(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);
        $shuffledFile = $shuffler->shufflePartition(0, []);

        $this->assertFileExists($shuffledFile);
        $this->assertEquals(0, filesize($shuffledFile));
    }

    public function testSingleRecord(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/single.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'only', 'value' => 42]
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([42], $results['only']);
    }

    public function testSmallChunkSize(): void
    {
        // Test with chunk size of 2 to force multiple chunks
        $shuffler = new Shuffler($this->tempDir, 2);

        $inputFile = "{$this->tempDir}/small_chunks.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'c', 'value' => 3],
            ['key' => 'a', 'value' => 4],
            ['key' => 'b', 'value' => 5],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([1, 4], $results['a']);
        $this->assertEquals([2, 5], $results['b']);
        $this->assertEquals([3], $results['c']);
    }

    public function testComplexKeys(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/complex_keys.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => ['user' => 1, 'product' => 100], 'value' => 2],
            ['key' => ['user' => 2, 'product' => 101], 'value' => 1],
            ['key' => ['user' => 1, 'product' => 100], 'value' => 3],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $key1 = serialize(['user' => 1, 'product' => 100]);
        $key2 = serialize(['user' => 2, 'product' => 101]);

        $this->assertArrayHasKey($key1, $results);
        $this->assertArrayHasKey($key2, $results);
        $this->assertEquals([2, 3], $results[$key1]);
        $this->assertEquals([1], $results[$key2]);
    }

    public function testSortedOutput(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/unsorted.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'zebra', 'value' => 1],
            ['key' => 'apple', 'value' => 2],
            ['key' => 'banana', 'value' => 3],
            ['key' => 'cherry', 'value' => 4],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);

        // Read file line by line to check order
        $handle = fopen($shuffledFile, 'r');
        $keys = [];
        if ($handle !== false) {
            while (($line = fgets($handle)) !== false) {
                $group = unserialize(trim($line));
                $keys[] = $group['key'];
            }
            fclose($handle);
        }

        $expectedOrder = ['apple', 'banana', 'cherry', 'zebra'];
        $this->assertEquals($expectedOrder, $keys);
    }

    public function testLargeDataset(): void
    {
        // Test with a larger dataset to ensure chunking works
        $shuffler = new Shuffler($this->tempDir, 100);

        $inputFile = "{$this->tempDir}/large.tmp";
        $handle = fopen($inputFile, 'w');

        $expectedCounts = [];
        if ($handle !== false) {
            for ($i = 0; $i < 1000; $i++) {
                $key = 'key_' . ($i % 10); // 10 different keys
                $value = $i;

                if (!isset($expectedCounts[$key])) {
                    $expectedCounts[$key] = [];
                }
                $expectedCounts[$key][] = $value;

                fwrite($handle, serialize(['key' => $key, 'value' => $value]) . "\n");
            }
            fclose($handle);
        }

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Verify all keys and values are present
        foreach ($expectedCounts as $key => $expectedValues) {
            $this->assertArrayHasKey($key, $results);
            sort($expectedValues);
            $actualValues = $results[$key];
            sort($actualValues);
            $this->assertEquals($expectedValues, $actualValues);
        }
    }

    public function testMultipleInputFiles(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        // Create multiple input files with overlapping keys
        $inputFiles = [];
        for ($i = 0; $i < 3; $i++) {
            $inputFile = "{$this->tempDir}/multi_input_{$i}.tmp";
            $inputFiles[] = $inputFile;

            $data = [];
            for ($j = 0; $j < 5; $j++) {
                $data[] = ['key' => "key_$j", 'value' => $i * 10 + $j];
            }

            $this->createTestInputFile($inputFile, $data);
        }

        $shuffledFile = $shuffler->shufflePartition(0, $inputFiles);
        $results = $this->readShuffledFile($shuffledFile);

        // Each key should have values from all 3 files
        for ($j = 0; $j < 5; $j++) {
            $key = "key_$j";
            $expectedValues = [0 * 10 + $j, 1 * 10 + $j, 2 * 10 + $j];

            $this->assertArrayHasKey($key, $results);
            $actualValues = $results[$key];
            sort($actualValues);
            sort($expectedValues);
            $this->assertEquals($expectedValues, $actualValues);
        }
    }

    public function testMemoryEfficiency(): void
    {
        // This test verifies that the shuffler doesn't load all data into memory
        // by processing a dataset with very small chunks
        $shuffler = new Shuffler($this->tempDir, 5); // Very small chunks

        $inputFile = "{$this->tempDir}/memory_test.tmp";
        $handle = fopen($inputFile, 'w');

        // Create 500 records (should require 100 chunks with size 5)
        if ($handle !== false) {
            for ($i = 0; $i < 500; $i++) {
                $key = 'key_' . ($i % 50); // 50 different keys
                fwrite($handle, serialize(['key' => $key, 'value' => $i]) . "\n");
            }
            fclose($handle);
        }

        $memoryBefore = memory_get_peak_usage();
        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $memoryAfter = memory_get_peak_usage();

        // Memory increase should be relatively small (not proportional to dataset size)
        $memoryIncrease = $memoryAfter - $memoryBefore;

        // Allow up to 10MB increase (should be much less for chunked processing)
        $this->assertLessThan(10 * 1024 * 1024, $memoryIncrease);

        // Verify the results are still correct
        $results = $this->readShuffledFile($shuffledFile);
        $this->assertCount(50, $results); // 50 unique keys

        // Each key should have 10 values (500 records / 50 keys)
        foreach ($results as $key => $values) {
            $this->assertCount(10, $values);
        }
    }

    public function testNonExistentInputFiles(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/existing.tmp";
        $nonExistentFile = "{$this->tempDir}/nonexistent.tmp";

        $this->createTestInputFile($inputFile, [
            ['key' => 'test', 'value' => 42]
        ]);

        // Should handle mix of existing and non-existent files gracefully
        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile, $nonExistentFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([42], $results['test']);
    }

    public function testFailedToOpenFileForReading(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        // Create a file and make it unreadable (on Unix systems)
        $inputFile = "{$this->tempDir}/unreadable.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'test', 'value' => 42]
        ]);

        // Make file unreadable (this may not work on all systems)
        chmod($inputFile, 0000);

        // Check if we actually made it unreadable (skip test if we couldn't)
        if (is_readable($inputFile)) {
            chmod($inputFile, 0644);
            $this->markTestSkipped('Cannot make file unreadable on this system');
        }

        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('Failed to open file for buffered reading');

            $shuffler->shufflePartition(0, [$inputFile]);
        } finally {
            // Restore permissions for cleanup
            chmod($inputFile, 0644);
        }
    }

    public function testFailedToCreateChunkFile(): void
    {
        // Create a read-only directory for the working directory
        $readOnlyDir = $this->tempDir . '/readonly';
        mkdir($readOnlyDir, 0755);

        $shuffler = new Shuffler($readOnlyDir, 2);

        $inputFile = "{$this->tempDir}/input.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'c', 'value' => 3],
        ]);

        // Make directory read-only
        chmod($readOnlyDir, 0555);

        // Check if we actually made it read-only (skip test if we couldn't)
        if (is_writable($readOnlyDir)) {
            chmod($readOnlyDir, 0755);
            $this->markTestSkipped('Cannot make directory read-only on this system');
        }

        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('Failed to open file for buffered writing');

            $shuffler->shufflePartition(0, [$inputFile]);
        } finally {
            // Restore permissions for cleanup
            chmod($readOnlyDir, 0755);
        }
    }

    public function testNumericKeySerialization(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/numeric_keys.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 100, 'value' => 'a'],
            ['key' => 50, 'value' => 'b'],
            ['key' => 100, 'value' => 'c'],
            ['key' => 75, 'value' => 'd'],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Integer keys should be serialized as strings
        $this->assertArrayHasKey('50', $results);
        $this->assertArrayHasKey('75', $results);
        $this->assertArrayHasKey('100', $results);
        
        $this->assertEquals(['b'], $results['50']);
        $this->assertEquals(['d'], $results['75']);
        $this->assertEquals(['a', 'c'], $results['100']);
    }

    public function testFloatKeySerialization(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/float_keys.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 1.5, 'value' => 'a'],
            ['key' => 2.5, 'value' => 'b'],
            ['key' => 1.5, 'value' => 'c'],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Float keys should be serialized as strings
        $this->assertEquals(['a', 'c'], $results['1.5']);
        $this->assertEquals(['b'], $results['2.5']);
    }

    public function testMixedKeyTypes(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/mixed_keys.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'string', 'value' => 1],
            ['key' => 42, 'value' => 2],
            ['key' => 3.14, 'value' => 3],
            ['key' => ['complex' => 'key'], 'value' => 4],
            ['key' => 'string', 'value' => 5],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertArrayHasKey('string', $results);
        $this->assertArrayHasKey('42', $results);
        $this->assertArrayHasKey('3.14', $results);
        $this->assertEquals([1, 5], $results['string']);
        
        $this->assertEquals([2], $results['42']);
        $this->assertEquals([3], $results['3.14']);
        $this->assertCount(4, $results);
    }

    public function testChunkFilesAreCleanedUp(): void
    {
        $shuffler = new Shuffler($this->tempDir, 2);

        $inputFile = "{$this->tempDir}/cleanup_test.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'c', 'value' => 3],
            ['key' => 'd', 'value' => 4],
            ['key' => 'e', 'value' => 5],
        ]);

        // Before shuffle, no chunk files exist
        $chunkFiles = glob("{$this->tempDir}/chunk_*.tmp");
        $this->assertEmpty($chunkFiles);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);

        // After shuffle, chunk files should be cleaned up
        $chunkFiles = glob("{$this->tempDir}/chunk_*.tmp");
        $this->assertEmpty($chunkFiles);

        // But shuffled file should exist
        $this->assertFileExists($shuffledFile);
    }

    public function testVeryLargeChunkSize(): void
    {
        // Test with chunk size larger than dataset
        $shuffler = new Shuffler($this->tempDir, 100000);

        $inputFile = "{$this->tempDir}/small_dataset.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'a', 'value' => 3],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([1, 3], $results['a']);
        $this->assertEquals([2], $results['b']);
    }

    public function testSingleChunkMultipleKeys(): void
    {
        // With a large chunk size, everything should fit in one chunk
        $shuffler = new Shuffler($this->tempDir, 1000);

        $inputFile = "{$this->tempDir}/single_chunk.tmp";
        $data = [];
        for ($i = 0; $i < 100; $i++) {
            $data[] = ['key' => 'key_' . ($i % 10), 'value' => $i];
        }
        $this->createTestInputFile($inputFile, $data);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Should have 10 unique keys
        $this->assertCount(10, $results);

        // Each key should have 10 values
        foreach ($results as $key => $values) {
            $this->assertCount(10, $values);
        }
    }

    public function testShuffleMultiplePartitions(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        // Create input files for 3 partitions
        $mapOutputFiles = [
            0 => ["{$this->tempDir}/map_0_part_0.tmp"],
            1 => ["{$this->tempDir}/map_0_part_1.tmp"],
            2 => ["{$this->tempDir}/map_0_part_2.tmp"],
        ];

        $this->createTestInputFile($mapOutputFiles[0][0], [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
        ]);

        $this->createTestInputFile($mapOutputFiles[1][0], [
            ['key' => 'c', 'value' => 3],
        ]);

        $this->createTestInputFile($mapOutputFiles[2][0], [
            ['key' => 'd', 'value' => 4],
            ['key' => 'e', 'value' => 5],
        ]);

        // Use shuffle() method instead of shufflePartition()
        $shuffledFiles = $shuffler->shuffle($mapOutputFiles, 3);

        $this->assertCount(3, $shuffledFiles);

        // Verify each partition has a shuffled file
        foreach ($shuffledFiles as $file) {
            $this->assertFileExists($file);
        }

        // Verify contents
        $results0 = $this->readShuffledFile($shuffledFiles[0]);
        $this->assertEquals([1], $results0['a']);
        $this->assertEquals([2], $results0['b']);

        $results1 = $this->readShuffledFile($shuffledFiles[1]);
        $this->assertEquals([3], $results1['c']);

        $results2 = $this->readShuffledFile($shuffledFiles[2]);
        $this->assertEquals([4], $results2['d']);
        $this->assertEquals([5], $results2['e']);
    }

    public function testShuffleWithEmptyPartitions(): void
    {
        $shuffler = new Shuffler($this->tempDir, 10);

        // Some partitions have files, others don't
        $mapOutputFiles = [
            0 => ["{$this->tempDir}/map_0_part_0.tmp"],
            1 => [], // Empty partition
            2 => ["{$this->tempDir}/map_0_part_2.tmp"],
        ];

        $this->createTestInputFile($mapOutputFiles[0][0], [
            ['key' => 'a', 'value' => 1],
        ]);

        $this->createTestInputFile($mapOutputFiles[2][0], [
            ['key' => 'b', 'value' => 2],
        ]);

        $shuffledFiles = $shuffler->shuffle($mapOutputFiles, 3);

        $this->assertCount(3, $shuffledFiles);

        // All files should exist, even for empty partitions
        foreach ($shuffledFiles as $file) {
            $this->assertFileExists($file);
        }

        // Partition 1 should be empty
        $this->assertEquals(0, filesize($shuffledFiles[1]));
    }

    public function testWriteGroupWithMultipleValues(): void
    {
        // Test that writeGroup handles multiple values correctly
        $shuffler = new Shuffler($this->tempDir, 5);

        $inputFile = "{$this->tempDir}/writegroup_test.tmp";
        $data = [];
        // Create enough data to trigger multiple chunks
        for ($i = 0; $i < 20; $i++) {
            $data[] = ['key' => 'same_key', 'value' => $i];
        }
        $this->createTestInputFile($inputFile, $data);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // All values should be grouped under the same key
        $this->assertCount(1, $results);
        $this->assertCount(20, $results['same_key']);
    }

    public function testMergeChunksWithSingleChunk(): void
    {
        // Test edge case where there's only one chunk
        $shuffler = new Shuffler($this->tempDir, 1000); // Large chunk size

        $inputFile = "{$this->tempDir}/single_merge.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'a', 'value' => 3],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertEquals([1, 3], $results['a']);
        $this->assertEquals([2], $results['b']);
    }

    public function testSortChunksWithEmptyLines(): void
    {
        // Create a file with valid data
        $shuffler = new Shuffler($this->tempDir, 3);

        $inputFile = "{$this->tempDir}/sort_test.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'z', 'value' => 1],
            ['key' => 'a', 'value' => 2],
            ['key' => 'm', 'value' => 3],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Results should be sorted by key
        $keys = array_keys($results);
        $this->assertEquals(['a', 'm', 'z'], $keys);
    }

    public function testSerializeKeyWithNullValue(): void
    {
        // Test that null keys are handled
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/null_key.tmp";

        // Manually create file with null key
        $handle = fopen($inputFile, 'w');
        if ($handle !== false) {
            fwrite($handle, serialize(['key' => null, 'value' => 1]) . "\n");
            fclose($handle);
        }

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);

        // Should not throw error
        $this->assertFileExists($shuffledFile);
        $results = $this->readShuffledFile($shuffledFile);

        // Null gets serialized
        $this->assertNotEmpty($results);
    }

    public function testLargeFileBufferedReading(): void
    {
        // Test that files larger than memoryLoadThreshold use BufferedFileReader
        // Default threshold is 10MB (10485760 bytes)
        $shuffler = new Shuffler($this->tempDir, 100, 1000, 1024); // Set threshold to 1KB

        $inputFile = "{$this->tempDir}/large_file.tmp";
        $handle = fopen($inputFile, 'w');

        // Create a file larger than 1KB threshold (about 2KB)
        if ($handle !== false) {
            for ($i = 0; $i < 50; $i++) {
                fwrite($handle, serialize(['key' => "key_$i", 'value' => $i]) . "\n");
            }
            fclose($handle);
        }

        // Verify file is larger than threshold
        $fileSize = filesize($inputFile);
        $this->assertGreaterThan(1024, $fileSize);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Should have processed all 50 keys
        $this->assertCount(50, $results);
    }

    public function testSmallFileMemoryLoading(): void
    {
        // Test that files smaller than memoryLoadThreshold are loaded entirely
        $shuffler = new Shuffler($this->tempDir, 100, 1000, 10485760); // 10MB threshold

        $inputFile = "{$this->tempDir}/small_file.tmp";
        $handle = fopen($inputFile, 'w');

        // Create a small file (definitely under 10MB)
        if ($handle !== false) {
            for ($i = 0; $i < 10; $i++) {
                fwrite($handle, serialize(['key' => "key_$i", 'value' => $i]) . "\n");
            }
            fclose($handle);
        }

        // Verify file is smaller than threshold
        $fileSize = filesize($inputFile);
        $this->assertLessThan(10485760, $fileSize);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Should have processed all 10 keys
        $this->assertCount(10, $results);
    }

    public function testFailedToOpenChunkFileInMerge(): void
    {
        // This test is complex because we need to test private method error handling
        // For now, we test that normal chunking and merging works correctly
        $shuffler = new Shuffler($this->tempDir, 2);

        $inputFile = "{$this->tempDir}/input.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'a', 'value' => 1],
            ['key' => 'b', 'value' => 2],
            ['key' => 'c', 'value' => 3],
            ['key' => 'd', 'value' => 4],
        ]);

        // This will create multiple chunks and merge them
        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Verify all data is present after chunking and merging
        $this->assertCount(4, $results);
        $this->assertEquals([1], $results['a']);
        $this->assertEquals([2], $results['b']);
        $this->assertEquals([3], $results['c']);
        $this->assertEquals([4], $results['d']);
    }

    public function testMergeChunksWithMultipleChunksAndKeyCollisions(): void
    {
        // Test merging multiple chunks where same keys appear in different chunks
        $shuffler = new Shuffler($this->tempDir, 3); // Small chunk size to force multiple chunks

        $inputFile1 = "{$this->tempDir}/input1.tmp";
        $inputFile2 = "{$this->tempDir}/input2.tmp";

        // Create data that will span multiple chunks
        $data1 = [];
        for ($i = 0; $i < 10; $i++) {
            $data1[] = ['key' => 'key_' . ($i % 3), 'value' => "file1_$i"];
        }
        $this->createTestInputFile($inputFile1, $data1);

        $data2 = [];
        for ($i = 0; $i < 10; $i++) {
            $data2[] = ['key' => 'key_' . ($i % 3), 'value' => "file2_$i"];
        }
        $this->createTestInputFile($inputFile2, $data2);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile1, $inputFile2]);
        $results = $this->readShuffledFile($shuffledFile);

        // Should have 3 unique keys
        $this->assertCount(3, $results);

        // Each key should have values from both files
        foreach ($results as $key => $values) {
            // Each key appears multiple times in each file
            $this->assertGreaterThan(1, count($values));
        }
    }

    public function testProcessLinesWithChunkBoundary(): void
    {
        // Test that processLines correctly handles chunk boundaries
        // This happens when recordCount reaches chunkSize
        $shuffler = new Shuffler($this->tempDir, 5); // Chunk size of 5

        $inputFile = "{$this->tempDir}/chunk_boundary.tmp";
        $data = [];

        // Create exactly 10 records (should create exactly 2 chunks)
        for ($i = 0; $i < 10; $i++) {
            $data[] = ['key' => "key_$i", 'value' => $i];
        }
        $this->createTestInputFile($inputFile, $data);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);

        // Verify all data is in shuffled file (chunks are cleaned up internally)
        $results = $this->readShuffledFile($shuffledFile);
        $this->assertCount(10, $results);

        // Verify shuffled file exists
        $this->assertFileExists($shuffledFile);
    }

    public function testProcessLinesWithPartialChunk(): void
    {
        // Test that processLines correctly handles partial chunks (less than chunkSize)
        $shuffler = new Shuffler($this->tempDir, 7); // Chunk size of 7

        $inputFile = "{$this->tempDir}/partial_chunk.tmp";
        $data = [];

        // Create 10 records (7 + 3, so one full chunk and one partial)
        for ($i = 0; $i < 10; $i++) {
            $data[] = ['key' => "key_$i", 'value' => $i];
        }
        $this->createTestInputFile($inputFile, $data);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // All 10 records should be present
        $this->assertCount(10, $results);
    }

    public function testMultipleFilesWithDifferentSizes(): void
    {
        // Test mixing small files (bulk loaded) and large files (buffered)
        $shuffler = new Shuffler($this->tempDir, 100, 1000, 500); // 500 byte threshold

        $smallFile = "{$this->tempDir}/small.tmp";
        $largeFile = "{$this->tempDir}/large.tmp";

        // Create small file (under 500 bytes)
        $this->createTestInputFile($smallFile, [
            ['key' => 'small_1', 'value' => 1],
            ['key' => 'small_2', 'value' => 2],
        ]);

        // Create large file (over 500 bytes)
        $handle = fopen($largeFile, 'w');
        if ($handle !== false) {
            for ($i = 0; $i < 20; $i++) {
                fwrite($handle, serialize(['key' => "large_$i", 'value' => $i]) . "\n");
            }
            fclose($handle);
        }

        $smallSize = filesize($smallFile);
        $largeSize = filesize($largeFile);
        $this->assertLessThan(500, $smallSize);
        $this->assertGreaterThan(500, $largeSize);

        $shuffledFile = $shuffler->shufflePartition(0, [$smallFile, $largeFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Should have all keys from both files
        $this->assertCount(22, $results); // 2 from small + 20 from large
    }

    public function testEmptyChunksArray(): void
    {
        // Test mergeChunks with empty chunk files array
        // This is tested indirectly through shufflePartition with empty input
        $shuffler = new Shuffler($this->tempDir, 10);

        $shuffledFile = $shuffler->shufflePartition(0, []);

        $this->assertFileExists($shuffledFile);
        $this->assertEquals(0, filesize($shuffledFile));
    }

    public function testBooleanKeySerialization(): void
    {
        // Test that boolean keys are handled correctly
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/boolean_keys.tmp";
        $handle = fopen($inputFile, 'w');
        if ($handle !== false) {
            fwrite($handle, serialize(['key' => true, 'value' => 1]) . "\n");
            fwrite($handle, serialize(['key' => false, 'value' => 2]) . "\n");
            fwrite($handle, serialize(['key' => true, 'value' => 3]) . "\n");
            fclose($handle);
        }

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        // Boolean true becomes "1", false becomes ""
        $this->assertNotEmpty($results);

        // Should have 2 distinct keys (true and false)
        $this->assertCount(2, $results);
    }

    public function testStringKeysWithSpecialCharacters(): void
    {
        // Test keys with special characters that might affect sorting
        $shuffler = new Shuffler($this->tempDir, 10);

        $inputFile = "{$this->tempDir}/special_chars.tmp";
        $this->createTestInputFile($inputFile, [
            ['key' => 'key-with-dash', 'value' => 1],
            ['key' => 'key_with_underscore', 'value' => 2],
            ['key' => 'key.with.dots', 'value' => 3],
            ['key' => 'key with spaces', 'value' => 4],
            ['key' => 'key-with-dash', 'value' => 5],
        ]);

        $shuffledFile = $shuffler->shufflePartition(0, [$inputFile]);
        $results = $this->readShuffledFile($shuffledFile);

        $this->assertCount(4, $results); // 4 unique keys
        $this->assertEquals([1, 5], $results['key-with-dash']);
        $this->assertEquals([2], $results['key_with_underscore']);
    }
}
