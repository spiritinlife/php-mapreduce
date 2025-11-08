<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\Utils\BufferedFileWriter;

class BufferedFileWriterTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/buffered_writer_test_' . uniqid();
        mkdir($this->tempDir, 0755, true);
    }

    protected function tearDown(): void
    {
        if (is_dir($this->tempDir)) {
            $files = scandir($this->tempDir);
            if ($files !== false) {
                $files = array_diff($files, ['.', '..']);
                foreach ($files as $file) {
                    @unlink($this->tempDir . '/' . $file);
                }
            }
            @rmdir($this->tempDir);
        }
    }

    public function testBasicWriting(): void
    {
        $filename = $this->tempDir . '/test.txt';
        $writer = new BufferedFileWriter($filename, 10);

        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");
        $writer->writeLine("line3\n");
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\nline3\n", $contents);
    }

    public function testAutoFlushWhenBufferFull(): void
    {
        $filename = $this->tempDir . '/test_autoflush.txt';
        $writer = new BufferedFileWriter($filename, 3); // Buffer size = 3

        // Write 3 lines - should trigger auto-flush
        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");
        $writer->writeLine("line3\n");

        // File should have content even before close (auto-flushed)
        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\nline3\n", $contents);

        // Write more lines
        $writer->writeLine("line4\n");
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\nline3\nline4\n", $contents);
    }

    public function testManualFlush(): void
    {
        $filename = $this->tempDir . '/test_manual_flush.txt';
        $writer = new BufferedFileWriter($filename, 100);

        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");

        // Manually flush before buffer is full
        $writer->flush();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\n", $contents);

        $writer->close();
    }

    public function testCloseFlushesRemainingData(): void
    {
        $filename = $this->tempDir . '/test_close_flush.txt';
        $writer = new BufferedFileWriter($filename, 100);

        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");

        // Close should flush remaining buffered data
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\n", $contents);
    }

    public function testMultipleWrites(): void
    {
        $filename = $this->tempDir . '/test_multiple.txt';
        $writer = new BufferedFileWriter($filename, 10);

        for ($i = 1; $i <= 100; $i++) {
            $writer->writeLine("line{$i}\n");
        }
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertIsString($contents);
        $lines = explode("\n", trim($contents));
        $this->assertCount(100, $lines);
        $this->assertEquals("line1", $lines[0]);
        $this->assertEquals("line100", $lines[99]);
    }

    public function testLargeBufferSize(): void
    {
        $filename = $this->tempDir . '/test_large_buffer.txt';
        $writer = new BufferedFileWriter($filename, 10000);

        for ($i = 1; $i <= 100; $i++) {
            $writer->writeLine("line{$i}\n");
        }
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertIsString($contents);
        $lines = explode("\n", trim($contents));
        $this->assertCount(100, $lines);
    }

    public function testSmallBufferSize(): void
    {
        $filename = $this->tempDir . '/test_small_buffer.txt';
        $writer = new BufferedFileWriter($filename, 1);

        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");
        $writer->writeLine("line3\n");
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\nline3\n", $contents);
    }

    public function testEmptyFlush(): void
    {
        $filename = $this->tempDir . '/test_empty_flush.txt';
        $writer = new BufferedFileWriter($filename, 10);

        // Flush with no data written
        $writer->flush();
        $writer->flush(); // Multiple empty flushes

        $writer->writeLine("line1\n");
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\n", $contents);
    }

    public function testMultipleCloses(): void
    {
        $filename = $this->tempDir . '/test_multiple_close.txt';
        $writer = new BufferedFileWriter($filename, 10);

        $writer->writeLine("line1\n");
        $writer->close();

        // Second close should be safe (no-op)
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\n", $contents);
    }

    public function testGetFilename(): void
    {
        $filename = $this->tempDir . '/test_getfilename.txt';
        $writer = new BufferedFileWriter($filename, 10);

        $this->assertEquals($filename, $writer->getFilename());
        $writer->close();
    }

    public function testFileNotWritable(): void
    {
        // Create a read-only directory
        $readOnlyDir = $this->tempDir . '/readonly';
        mkdir($readOnlyDir, 0755);
        chmod($readOnlyDir, 0555);

        if (is_writable($readOnlyDir)) {
            chmod($readOnlyDir, 0755);
            $this->markTestSkipped('Cannot make directory read-only on this system');
        }

        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('Failed to open file for buffered writing');

            new BufferedFileWriter($readOnlyDir . '/test.txt', 10);
        } finally {
            chmod($readOnlyDir, 0755);
        }
    }

    public function testDestructorFlushesData(): void
    {
        $filename = $this->tempDir . '/test_destructor.txt';

        $writer = new BufferedFileWriter($filename, 100);
        $writer->writeLine("line1\n");
        $writer->writeLine("line2\n");
        // Unset to trigger destructor
        unset($writer);

        // Force garbage collection to ensure destructor runs
        gc_collect_cycles();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1\nline2\n", $contents);
    }

    public function testWriteWithoutNewline(): void
    {
        $filename = $this->tempDir . '/test_no_newline.txt';
        $writer = new BufferedFileWriter($filename, 10);

        // writeLine doesn't enforce newlines - user controls format
        $writer->writeLine("line1");
        $writer->writeLine("line2");
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals("line1line2", $contents);
    }

    public function testLargeSingleLine(): void
    {
        $filename = $this->tempDir . '/test_large_line.txt';
        $writer = new BufferedFileWriter($filename, 10);

        // Write a line larger than buffer size
        $largeLine = str_repeat("x", 10000) . "\n";
        $writer->writeLine($largeLine);
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals($largeLine, $contents);
    }

    public function testBinaryData(): void
    {
        $filename = $this->tempDir . '/test_binary.txt';
        $writer = new BufferedFileWriter($filename, 10);

        $binaryData = "\x00\x01\x02\x03\xFF\xFE\n";
        $writer->writeLine($binaryData);
        $writer->close();

        $contents = file_get_contents($filename);
        $this->assertEquals($binaryData, $contents);
    }
}
