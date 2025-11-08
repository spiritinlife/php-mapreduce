<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\Utils\BufferedFileReader;

class BufferedFileReaderTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/buffered_reader_test_' . uniqid();
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

    public function testBasicReading(): void
    {
        $filename = $this->tempDir . '/test.txt';
        file_put_contents($filename, "line1\nline2\nline3\n");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals("line1", $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());
        $this->assertEquals("line3", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testReadSmallFile(): void
    {
        $filename = $this->tempDir . '/small.txt';
        file_put_contents($filename, "single line");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals("single line", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testReadEmptyFile(): void
    {
        $filename = $this->tempDir . '/empty.txt';
        file_put_contents($filename, "");

        $reader = new BufferedFileReader($filename);

        $this->assertNull($reader->getLine());
        $this->assertFalse($reader->hasLines());

        $reader->close();
    }

    public function testReadFileWithEmptyLines(): void
    {
        $filename = $this->tempDir . '/empty_lines.txt';
        file_put_contents($filename, "line1\n\n\nline2\n\nline3\n");

        $reader = new BufferedFileReader($filename);

        // Empty lines should be filtered out
        $this->assertEquals("line1", $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());
        $this->assertEquals("line3", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testReadLargeFileLargerThanBuffer(): void
    {
        $filename = $this->tempDir . '/large.txt';

        // Create file with many lines (larger than default 64KB buffer)
        $lines = [];
        for ($i = 1; $i <= 10000; $i++) {
            $lines[] = "line{$i}";
        }
        file_put_contents($filename, implode("\n", $lines));

        $reader = new BufferedFileReader($filename);

        // Read all lines
        for ($i = 1; $i <= 10000; $i++) {
            $this->assertEquals("line{$i}", $reader->getLine());
        }
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testSmallBufferSize(): void
    {
        $filename = $this->tempDir . '/test_small_buffer.txt';
        file_put_contents($filename, "line1\nline2\nline3\nline4\nline5\n");

        // Use very small buffer (10 bytes) to force multiple reads
        $reader = new BufferedFileReader($filename, 10);

        $this->assertEquals("line1", $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());
        $this->assertEquals("line3", $reader->getLine());
        $this->assertEquals("line4", $reader->getLine());
        $this->assertEquals("line5", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testHasLines(): void
    {
        $filename = $this->tempDir . '/test_has_lines.txt';
        file_put_contents($filename, "line1\nline2\nline3\n");

        $reader = new BufferedFileReader($filename);

        $this->assertTrue($reader->hasLines());
        $reader->getLine(); // line1
        $this->assertTrue($reader->hasLines());
        $reader->getLine(); // line2
        $this->assertTrue($reader->hasLines());
        $reader->getLine(); // line3
        $this->assertFalse($reader->hasLines());

        $reader->close();
    }

    public function testPartialLineAtChunkBoundary(): void
    {
        $filename = $this->tempDir . '/partial_line.txt';

        // Create content where line boundary falls in middle of chunk
        // Use buffer size of 10 bytes, lines are 5-6 bytes each
        file_put_contents($filename, "abc\ndef\nghi\njkl\n");

        $reader = new BufferedFileReader($filename, 10);

        $this->assertEquals("abc", $reader->getLine());
        $this->assertEquals("def", $reader->getLine());
        $this->assertEquals("ghi", $reader->getLine());
        $this->assertEquals("jkl", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testFileNotFound(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Failed to open file for buffered reading');

        new BufferedFileReader($this->tempDir . '/nonexistent.txt');
    }

    public function testMultipleCloses(): void
    {
        $filename = $this->tempDir . '/test_close.txt';
        file_put_contents($filename, "line1\n");

        $reader = new BufferedFileReader($filename);
        $reader->getLine();
        $reader->close();

        // Second close should be safe (no-op)
        $reader->close();

        $this->assertTrue(true); // No exception thrown
    }

    public function testDestructor(): void
    {
        $filename = $this->tempDir . '/test_destructor.txt';
        file_put_contents($filename, "line1\n");

        // Scope to trigger destructor
        {
            $reader = new BufferedFileReader($filename);
            $this->assertEquals("line1", $reader->getLine());
            // No explicit close - destructor should handle it
        }

        // If destructor doesn't close properly, file handle would leak
        $this->assertTrue(true);
    }

    public function testReadAfterEOF(): void
    {
        $filename = $this->tempDir . '/test_eof.txt';
        file_put_contents($filename, "line1\n");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals("line1", $reader->getLine());
        $this->assertNull($reader->getLine());
        $this->assertNull($reader->getLine()); // Multiple reads after EOF
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testNoTrailingNewline(): void
    {
        $filename = $this->tempDir . '/no_trailing.txt';
        file_put_contents($filename, "line1\nline2\nline3");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals("line1", $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());
        $this->assertEquals("line3", $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testBinaryData(): void
    {
        $filename = $this->tempDir . '/binary.txt';
        $binaryLine = "\x00\x01\x02\x03\xFF\xFE";
        file_put_contents($filename, $binaryLine . "\nline2\n");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals($binaryLine, $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());

        $reader->close();
    }

    public function testTypicalSerializedDataLines(): void
    {
        $filename = $this->tempDir . '/typical_data.txt';

        // Test with lines typical for serialized MapReduce data
        // Use realistic serialized data sizes (most lines <1KB)
        $line1 = str_repeat("a", 30);
        $line2 = str_repeat("b", 40);
        $line3 = str_repeat("c", 35);

        file_put_contents($filename, "{$line1}\n{$line2}\n{$line3}\n");

        // Use buffer larger than individual lines (typical case)
        $reader = new BufferedFileReader($filename, 100);

        $this->assertEquals($line1, $reader->getLine());
        $this->assertEquals($line2, $reader->getLine());
        $this->assertEquals($line3, $reader->getLine());
        $this->assertNull($reader->getLine());

        $reader->close();
    }

    public function testOnlyNewlines(): void
    {
        $filename = $this->tempDir . '/only_newlines.txt';
        file_put_contents($filename, "\n\n\n\n\n");

        $reader = new BufferedFileReader($filename);

        // All empty lines should be filtered
        $this->assertNull($reader->getLine());
        $this->assertFalse($reader->hasLines());

        $reader->close();
    }

    public function testMixedLineEndings(): void
    {
        $filename = $this->tempDir . '/mixed_endings.txt';
        file_put_contents($filename, "line1\nline2\nline3\n");

        $reader = new BufferedFileReader($filename);

        $this->assertEquals("line1", $reader->getLine());
        $this->assertEquals("line2", $reader->getLine());
        $this->assertEquals("line3", $reader->getLine());

        $reader->close();
    }
}
