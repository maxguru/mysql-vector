<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;
use PHPUnit\Framework\TestCase;

class VectorTableTest extends TestCase
{
    private $vectorTable;
    private $dimension = 384;
    private $testVectorAmount = 100;

    protected function setUp(): void
    {
        VectorTableTest::tearDownAfterClass();

        $mysqli = new \mysqli('db', 'db', 'db', 'db', 3306);

        // Check connection
        if ($mysqli->connect_error) {
            die("Connection failed: " . $mysqli->connect_error);
        }

        // Setup VectorTable for testing
        $this->vectorTable = new VectorTable($mysqli, 'test_table', $this->dimension);

        // Create required tables for testing
        $this->vectorTable->initialize();
    }

    private function getRandomVectors($count, $dimension) {
        $vecs = [];
        for ($i = 0; $i < $count; $i++) {
            for($j = 0; $j < $dimension; $j++) {
                $vecs[$i][$j] = 2 * (mt_rand(0, 1000) / 1000) - 1;
            }
        }
        return $vecs;
    }

    public function testGetVectorTableName()
    {
        $tableName = $this->vectorTable->getVectorTableName();
        $this->assertEquals('test_table_vectors', $tableName);
    }

    public function testUpsertSingle() {
        $this->vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(1, $this->dimension);

        $ids = [];

        echo "Inserting 1 vector...\n";
        $time = microtime(true);
        foreach ($vecs as $vec) {
            $ids[] = $this->vectorTable->upsert($vec);
        }
        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals(count($vecs), $this->vectorTable->count());
        $this->vectorTable->getConnection()->rollback();
    }

    public function testUpsert() {
        $this->vectorTable->getConnection()->begin_transaction();

        $lastId = 0;
        $vecArray = [];
        echo "Inserting $this->testVectorAmount vectors one-at-a-time...\n";
        $time = microtime(true);
        for($i = 0; $i < $this->testVectorAmount; $i++) {
            $vec = $this->getRandomVectors(1, $this->dimension)[0];
            $lastId = $this->vectorTable->upsert($vec);
            $vecArray[] = $vec;
        }

        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals($this->testVectorAmount, count($this->vectorTable->selectAll()));

        echo "Inserting another $this->testVectorAmount vectors in a batch...\n";
        $time = microtime(true);
        $this->vectorTable->batchInsert($vecArray);

        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals($this->testVectorAmount * 2, $this->vectorTable->count());

        $id = $lastId;
        $newVec = $this->getRandomVectors(1, $this->dimension)[0];
        $this->vectorTable->upsert($newVec, $id);
        $r = $this->vectorTable->select([$id]);
        $this->assertCount(1, $r);
        $this->assertEqualsWithDelta($newVec, $r[0]['vector'], 0.00001);

        $this->vectorTable->getConnection()->rollback();
    }

    public function testCosim() {
        $this->vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(2, $this->dimension);
        $dotProduct = 0;
        for ($i = 0; $i < count($vecs[0]); $i++) {
            $dotProduct += $vecs[0][$i] * $vecs[1][$i];
        }

        $this->assertEqualsWithDelta($dotProduct, $this->vectorTable->cosim($vecs[0], $vecs[1]), 0.0001);
    }

    public function testSelectAll() {
        $this->vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(10, $this->dimension);
        foreach ($vecs as $vec) {
            $this->vectorTable->upsert($vec);
        }

        $results = $this->vectorTable->selectAll();
        $this->assertSameSize($vecs, $results);

        $i = 0;
        foreach ($results as $result) {
            $this->assertEqualsWithDelta($vecs[$i], $result['vector'], 0.00001);
            $i++;
        }

        $this->vectorTable->getConnection()->rollback();
    }

    public function testVectorToHex() {
        $hex = $this->vectorTable->vectorToHex([0.5, 0.5, 0, 0, 0, 0.5]);
        $this->assertEqualsIgnoringCase('23', $hex);

        $hex = $this->vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 0,0,0,0,0,0,0,0,0,0,0,0]);
        $this->assertEqualsIgnoringCase('0f00', $hex);

        $hex = $this->vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 0,0,0,0,0,0,0,0,0,0,0,1]);
        $this->assertEqualsIgnoringCase('0f80', $hex);

        $hex = $this->vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 1,0,0,0,0,0,0,0,0,0,0,1]);
        $this->assertEqualsIgnoringCase('1f80', $hex);
    }

    /**
     * Test for the quantization bug fix (commit 6f7e3a6)
     * This test verifies that the vectorToHex method uses proper bit manipulation
     * instead of string operations, resulting in correct little-endian bit ordering.
     *
     * BUG DESCRIPTION:
     * The old implementation used string concatenation and big-endian bit ordering:
     * - Built binary string: '10101001'
     * - Converted via string operations: bindec('1010') + bindec('1001') -> 'A9'
     * - Added unnecessary padding: '00A9'
     *
     * The fixed implementation uses proper bit manipulation with little-endian ordering:
     * - Uses bitwise operations: char |= 1<<bit
     * - Builds bytes directly: chr(149) -> 0x95
     * - Uses bin2hex() for efficient conversion
     *
     * This test would FAIL with the old implementation and PASS with the new one.
     */
    public function testQuantizationBugFix() {
        // Test case 1: Simple 8-bit vector
        // Binary: 10101001 -> Little-endian: bit 0=1, bit 2=1, bit 4=1, bit 7=1
        // Expected: 0x95 (149 decimal)
        $vector1 = [1, -1, 1, 0, 1, -1, 0, 1];
        $hex1 = $this->vectorTable->vectorToHex($vector1);
        $this->assertEquals('95', $hex1, 'Simple 8-bit vector should produce correct little-endian result');

        // Test case 2: All zeros
        $vector2 = [0, 0, 0, 0, 0, 0, 0, 0];
        $hex2 = $this->vectorTable->vectorToHex($vector2);
        $this->assertEquals('00', $hex2, 'All zeros should produce 00');

        // Test case 3: All ones
        $vector3 = [1, 1, 1, 1, 1, 1, 1, 1];
        $hex3 = $this->vectorTable->vectorToHex($vector3);
        $this->assertEquals('ff', $hex3, 'All ones should produce ff');

        // Test case 4: Partial byte (4 bits)
        // Binary: 1101 -> Little-endian: bit 0=1, bit 1=1, bit 3=1
        // Expected: 0x0b (11 decimal: 1+2+8)
        $vector4 = [1, 1, 0, 1];
        $hex4 = $this->vectorTable->vectorToHex($vector4);
        $this->assertEquals('0b', $hex4, 'Partial byte should be handled correctly');

        // Test case 5: Cross byte boundary (9 bits)
        // First byte: bit 0=1 -> 0x01
        // Second byte: bit 0=1 -> 0x01
        $vector5 = [1, 0, 0, 0, 0, 0, 0, 0, 1];
        $hex5 = $this->vectorTable->vectorToHex($vector5);
        $this->assertEquals('0101', $hex5, 'Cross byte boundary should work correctly');

        // Test case 6: Verify consistency (same input should always produce same output)
        $vector6 = [1, -1, 1, 0, 1, -1, 0, 1];
        $hex6a = $this->vectorTable->vectorToHex($vector6);
        $hex6b = $this->vectorTable->vectorToHex($vector6);
        $this->assertEquals($hex6a, $hex6b, 'Results should be deterministic');

        // Test case 7: Large vector (typical embedding size)
        $vector7 = array_fill(0, 384, 0);
        $vector7[0] = 1;  // Set first bit
        $vector7[7] = 1;  // Set 8th bit (position 7)
        $vector7[383] = 1; // Set last bit
        $hex7 = $this->vectorTable->vectorToHex($vector7);

        // Should be 48 bytes (384 bits / 8)
        $this->assertEquals(96, strlen($hex7), 'Large vector should produce correct length hex string');

        // First byte should have bits 0 and 7 set: 0x81
        $firstByte = substr($hex7, 0, 2);
        $this->assertEquals('81', $firstByte, 'First byte should have correct bit pattern');

        // Last byte should have bit 7 set (bit 383 % 8 = 7): 0x80
        $lastByte = substr($hex7, -2);
        $this->assertEquals('80', $lastByte, 'Last byte should have correct bit pattern');
    }

    /**
     * Test that demonstrates the specific bug that was fixed
     * This test shows the difference between old (buggy) and new (fixed) behavior
     */
    public function testQuantizationBugFixComparison() {
        // This is what the OLD buggy implementation would have produced:
        // Vector [1, -1, 1, 0, 1, -1, 0, 1] -> binary string "10101001"
        // -> padded "10101001" -> split into ["1010", "1001"]
        // -> dechex(bindec("1010")) + dechex(bindec("1001")) -> "A" + "9" -> "A9"
        // -> padded to 4 chars -> "00A9"

        // The NEW fixed implementation produces:
        $vector = [1, -1, 1, 0, 1, -1, 0, 1];
        $actualResult = $this->vectorTable->vectorToHex($vector);

        // With proper bit manipulation and little-endian ordering: 0x95
        $expectedFixed = '95';
        $oldBuggyResult = '00a9'; // What the old implementation would have produced

        // Verify the fix is working (should NOT match old buggy result)
        $this->assertNotEquals($oldBuggyResult, strtolower($actualResult),
            'Result should NOT match the old buggy implementation');

        // Verify the fix produces the correct result
        $this->assertEquals($expectedFixed, $actualResult,
            'Result should match the fixed implementation with proper bit manipulation');

        // Additional verification: test the bit pattern manually
        // Vector [1, -1, 1, 0, 1, -1, 0, 1] should set bits: 0, 2, 4, 7
        // In little-endian: 1 + 4 + 16 + 128 = 149 = 0x95
        $expectedDecimal = 1 + 4 + 16 + 128; // bits 0, 2, 4, 7
        $actualDecimal = hexdec($actualResult);
        $this->assertEquals($expectedDecimal, $actualDecimal,
            'Decimal value should match expected bit pattern');
    }

    public function testSearch() {
        $multiples = 1;
        $this->vectorTable->getConnection()->begin_transaction();

        // Insert $this->testVectorAmount random vectors
        for($i = 0; $i < $multiples; $i++) {
            $vecs = $this->getRandomVectors($this->testVectorAmount, $this->dimension);
            $this->vectorTable->batchInsert($vecs);
        }

        // Let's insert a known vector
        $targetVector = array_fill(0, $this->dimension, 0.5);
        $this->vectorTable->upsert($targetVector);

        // Now, we search for this vector
        $searchAmount = $this->testVectorAmount * $multiples;
        echo "Searching for 1 vector among ($searchAmount) with binary quantization...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        // print time in format 00:00:00.000
        echo sprintf("Search completed in %.2f seconds\n", $time);

        // At least the first result should be our target vector or very close
        $firstResultVector = $results[0]['vector'];
        $firstResultSimilarity = $results[0]['similarity'];

        $this->assertEqualsWithDelta($targetVector, $firstResultVector, 0.00001, "The most similar vector should be the target vector itself");
        $this->assertEqualsWithDelta(1.0, $firstResultSimilarity, 0.001, "The similarity of the most similar vector should be the highest possible value");

        $this->vectorTable->getConnection()->rollback();
    }

    public function testDelete(): void {
        $this->vectorTable->getConnection()->begin_transaction();

        $ids = [];
        $vecs = $this->getRandomVectors(10, $this->dimension);
        foreach ($vecs as $vec) {
            $ids[] = $this->vectorTable->upsert($vec);
        }

        $this->assertEquals(count($ids), $this->vectorTable->count());

        foreach ($ids as $id) {
            $this->vectorTable->delete($id);
        }

        $this->assertEquals(0, $this->vectorTable->count());

        $this->vectorTable->getConnection()->rollback();
    }

    public static function tearDownAfterClass(): void
    {
        // Clean up the database and close connection
        $mysqli = new \mysqli('db', 'db', 'db', 'db', 3306);
        $vectorTable = new VectorTable($mysqli, 'test_table', 3);
        $mysqli->query("DROP TABLE IF EXISTS " . $vectorTable->getVectorTableName());
        $mysqli->query("DROP FUNCTION IF EXISTS COSIM");
        $mysqli->close();
    }

    protected function tearDown(): void
    {
        // Clean up the database and close connection
        $this->vectorTable->getConnection()->query("DROP TABLE IF EXISTS " . $this->vectorTable->getVectorTableName());
        $this->vectorTable->getConnection()->query("DROP FUNCTION IF EXISTS COSIM");
        $this->vectorTable->getConnection()->close();
    }

}
