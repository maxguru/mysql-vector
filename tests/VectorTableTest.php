<?php

namespace MHz\MysqlVector\Tests;

class VectorTableTest extends BaseVectorTest
{
    private $dimension = 384;
    private $testVectorAmount = 100;

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
        $vectorTable = $this->makeTable('vector_table_test', $this->dimension);
        $tableName = $vectorTable->getVectorTableName();
        $this->assertTrue(strpos($tableName, 'vector_table_test') !== false);
    }

    public function testUpsertSingle() {
        $vectorTable = $this->makeTable('upsert_single_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(1, $this->dimension);

        $ids = [];

        echo "Inserting 1 vector...\n";
        $time = microtime(true);
        foreach ($vecs as $vec) {
            $ids[] = $vectorTable->upsert($vec);
        }
        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals(count($vecs), $vectorTable->count());
        $vectorTable->getConnection()->rollback();
    }

    public function testUpsert() {
        $vectorTable = $this->makeTable('upsert_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $lastId = 0;
        $vecArray = [];
        echo "Inserting $this->testVectorAmount vectors one-at-a-time...\n";
        $time = microtime(true);
        for($i = 0; $i < $this->testVectorAmount; $i++) {
            $vec = $this->getRandomVectors(1, $this->dimension)[0];
            $lastId = $vectorTable->upsert($vec);
            $vecArray[] = $vec;
        }

        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals($this->testVectorAmount, count($vectorTable->selectAll()));

        echo "Inserting another $this->testVectorAmount vectors in a batch...\n";
        $time = microtime(true);
        $items = array_map(function($v) { return ['vector' => $v]; }, $vecArray);
        $vectorTable->batchInsert($items);

        $time = microtime(true) - $time;
        echo "Elapsed time: " . sprintf("%.2f", $time) . " seconds\n";

        $this->assertEquals($this->testVectorAmount * 2, $vectorTable->count());

        $id = $lastId;
        $newVec = $this->getRandomVectors(1, $this->dimension)[0];
        $vectorTable->upsert($newVec, null, $id);
        $r = $vectorTable->select([$id]);
        $this->assertCount(1, $r);
        // Verify that the stored vector matches what the normalize() method would produce
        $expectedNormalized = $vectorTable->normalize($newVec);
        // Fetch normalized_vector directly from the database and decode
        $tableName = $vectorTable->getVectorTableName();
        $stmt = self::$mysqli->prepare("SELECT normalized_vector FROM `$tableName` WHERE id = ?");
        $this->assertNotFalse($stmt, "Failed to prepare statement to fetch normalized_vector");
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $stmt->bind_result($blob);
        $this->assertTrue($stmt->fetch(), "Failed to fetch normalized_vector for id=$id");
        $stmt->close();
        $actualStored = $vectorTable->blobToVector($blob);
        // Stored vectors are encoded as float32; allow slightly larger tolerance
        $this->assertEqualsWithDelta($expectedNormalized, $actualStored, 1e-6, "Stored vector should match normalized() within float32 precision");

        $vectorTable->getConnection()->rollback();
    }

    public function testCosim() {
        $vectorTable = $this->makeTable('cosim_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(2, $this->dimension);

        // Calculate expected cosine similarity manually
        // Since cosim() normalizes inputs, we need to normalize our test vectors too
        $norm1 = $vectorTable->normalize($vecs[0]);
        $norm2 = $vectorTable->normalize($vecs[1]);

        $dotProduct = 0;
        for ($i = 0; $i < count($norm1); $i++) {
            $dotProduct += $norm1[$i] * $norm2[$i];
        }

        $this->assertEqualsWithDelta($dotProduct, $vectorTable->cosim($vecs[0], $vecs[1]), 0.0001);
    }

    public function testSelectAll() {
        $vectorTable = $this->makeTable('select_all_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $vecs = $this->getRandomVectors(10, $this->dimension);
        foreach ($vecs as $vec) {
            $vectorTable->upsert($vec);
        }

        $results = $vectorTable->selectAll();
        $this->assertSameSize($vecs, $results);

        // Results should include only id and metadata
        foreach ($results as $result) {
            $this->assertArrayHasKey('id', $result);
            $this->assertArrayHasKey('metadata', $result);
        }

        $vectorTable->getConnection()->rollback();
    }

    public function testVectorToHex() {
        $vectorTable = $this->makeTable('vector_to_hex_test', $this->dimension);

        $hex = $vectorTable->vectorToHex([0.5, 0.5, 0, 0, 0, 0.5]);
        $this->assertEqualsIgnoringCase('23', $hex);

        $hex = $vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 0,0,0,0,0,0,0,0,0,0,0,0]);
        $this->assertEqualsIgnoringCase('0f00', $hex);

        $hex = $vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 0,0,0,0,0,0,0,0,0,0,0,1]);
        $this->assertEqualsIgnoringCase('0f80', $hex);

        $hex = $vectorTable->vectorToHex([0.5, 0.5, 0.5, 0.5, 1,0,0,0,0,0,0,0,0,0,0,1]);
        $this->assertEqualsIgnoringCase('1f80', $hex);
    }

    public function testVectorToBlobAndBackRoundTrip()
    {
        $dimension = 16;
        // Directly instantiate VectorTable; no DB table creation needed for pure PHP methods
        $vectorTable = new \MHz\MysqlVector\VectorTable(self::$mysqli, 'blob_roundtrip', $dimension);

        // generate a vector with a range of magnitudes and signs
        $vec = [];
        for ($i = 0; $i < $dimension; $i++) {
            $vec[$i] = ($i % 2 === 0 ? 1 : -1) * ($i + 0.5) / $dimension; // deterministic
        }

        // Normalize to match library behavior on storage
        $norm = $vectorTable->normalize($vec);

        $blob = $vectorTable->vectorToBlob($norm);
        $decoded = $vectorTable->blobToVector($blob);

        // Float32 tolerance: allow small delta compared to double
        $this->assertEqualsWithDelta($norm, $decoded, 1e-6, 'Decoded vector should match original within float32 precision');
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
        $vectorTable = $this->makeTable('quantization_test', $this->dimension);

        // Test case 1: Simple 8-bit vector
        // Binary: 10101001 -> Little-endian: bit 0=1, bit 2=1, bit 4=1, bit 7=1
        // Expected: 0x95 (149 decimal)
        $vector1 = [1, -1, 1, 0, 1, -1, 0, 1];
        $hex1 = $vectorTable->vectorToHex($vector1);
        $this->assertEquals('95', $hex1, 'Simple 8-bit vector should produce correct little-endian result');

        // Test case 2: All zeros
        $vector2 = [0, 0, 0, 0, 0, 0, 0, 0];
        $hex2 = $vectorTable->vectorToHex($vector2);
        $this->assertEquals('00', $hex2, 'All zeros should produce 00');

        // Test case 3: All ones
        $vector3 = [1, 1, 1, 1, 1, 1, 1, 1];
        $hex3 = $vectorTable->vectorToHex($vector3);
        $this->assertEquals('ff', $hex3, 'All ones should produce ff');

        // Test case 4: Partial byte (4 bits)
        // Binary: 1101 -> Little-endian: bit 0=1, bit 1=1, bit 3=1
        // Expected: 0x0b (11 decimal: 1+2+8)
        $vector4 = [1, 1, 0, 1];
        $hex4 = $vectorTable->vectorToHex($vector4);
        $this->assertEquals('0b', $hex4, 'Partial byte should be handled correctly');

        // Test case 5: Cross byte boundary (9 bits)
        // First byte: bit 0=1 -> 0x01
        // Second byte: bit 0=1 -> 0x01
        $vector5 = [1, 0, 0, 0, 0, 0, 0, 0, 1];
        $hex5 = $vectorTable->vectorToHex($vector5);
        $this->assertEquals('0101', $hex5, 'Cross byte boundary should work correctly');

        // Test case 6: Verify consistency (same input should always produce same output)
        $vector6 = [1, -1, 1, 0, 1, -1, 0, 1];
        $hex6a = $vectorTable->vectorToHex($vector6);
        $hex6b = $vectorTable->vectorToHex($vector6);
        $this->assertEquals($hex6a, $hex6b, 'Results should be deterministic');

        // Test case 7: Large vector (typical embedding size)
        $vector7 = array_fill(0, 384, 0);
        $vector7[0] = 1;  // Set first bit
        $vector7[7] = 1;  // Set 8th bit (position 7)
        $vector7[383] = 1; // Set last bit
        $hex7 = $vectorTable->vectorToHex($vector7);

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
        $vectorTable = $this->makeTable('quantization_comparison_test', $this->dimension);

        // This is what the OLD buggy implementation would have produced:
        // Vector [1, -1, 1, 0, 1, -1, 0, 1] -> binary string "10101001"
        // -> padded "10101001" -> split into ["1010", "1001"]
        // -> dechex(bindec("1010")) + dechex(bindec("1001")) -> "A" + "9" -> "A9"
        // -> padded to 4 chars -> "00A9"

        // The NEW fixed implementation produces:
        $vector = [1, -1, 1, 0, 1, -1, 0, 1];
        $actualResult = $vectorTable->vectorToHex($vector);

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
        $vectorTable = $this->makeTable('search_test', $this->dimension);
        $multiples = 1;
        $vectorTable->getConnection()->begin_transaction();

        // Insert $this->testVectorAmount random vectors
        for($i = 0; $i < $multiples; $i++) {
            $vecs = $this->getRandomVectors($this->testVectorAmount, $this->dimension);
            $vectorTable->batchInsert(array_map(function($v){ return ['vector' => $v]; }, $vecs));
        }

        // Let's insert a known vector
        $targetVector = array_fill(0, $this->dimension, 0.5);
        $targetId = $vectorTable->upsert($targetVector);

        // Now, we search for this vector
        $searchAmount = $this->testVectorAmount * $multiples;
        echo "Searching for 1 vector among ($searchAmount) with binary quantization...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        // print time in format 00:00:00.000
        echo sprintf("Search completed in %.2f seconds\n", $time);

        // Validate that the top search result is the vector we inserted
        $this->assertEquals($targetId, $results[0]['id'], "The most similar vector should be the target vector's ID");
        $firstResultSimilarity = $results[0]['similarity'];

        // The similarity should be very high since we're searching for the same vector we inserted
        $this->assertGreaterThan(0.99, $firstResultSimilarity, "The similarity should be very high when searching for the same vector");

        $vectorTable->getConnection()->rollback();
    }

    public function testDelete(): void {
        $vectorTable = $this->makeTable('delete_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $ids = [];
        $vecs = $this->getRandomVectors(10, $this->dimension);
        foreach ($vecs as $vec) {
            $ids[] = $vectorTable->upsert($vec);
        }

        $this->assertEquals(count($ids), $vectorTable->count());

        foreach ($ids as $id) {
            $vectorTable->delete($id);
        }

        $this->assertEquals(0, $vectorTable->count());

        $vectorTable->getConnection()->rollback();
    }

    public function testConstructorAllowsMaxVarbinaryBoundDimension() {
        $dimension = \MHz\MysqlVector\VectorTable::MAX_DIMENSIONS; // VARBINARY(4*dim) limit
        $tableName = 'constructor_limit_ok_' . uniqid();

        // Should NOT throw; we do not initialize tables here
        $vt = new \MHz\MysqlVector\VectorTable(self::$mysqli, $tableName, $dimension);
        $this->assertInstanceOf(\MHz\MysqlVector\VectorTable::class, $vt);
    }

    public function testConstructorRejectsBeyondVarbinaryBoundDimension() {
        $dimension = \MHz\MysqlVector\VectorTable::MAX_DIMENSIONS + 1; // one above the max
        $tableName = 'constructor_limit_exceed_' . uniqid();

        try {
            new \MHz\MysqlVector\VectorTable(self::$mysqli, $tableName, $dimension);
            $this->fail('Expected InvalidArgumentException for exceeding VARBINARY-backed dimension limit');
        } catch (\InvalidArgumentException $e) {
            $msg = $e->getMessage();
            // Message should reflect the numeric max-dimension constraint
            $this->assertStringContainsString('Maximum supported dimension', $msg);
            $this->assertStringContainsString((string) \MHz\MysqlVector\VectorTable::MAX_DIMENSIONS, $msg);
        }
    }

    public function testSelectByMetadata(): void {
        // Create table with JSON path indexes for content_type and content_id
        $vectorTable = $this->makeTable('select_by_metadata_test', $this->dimension, ['$.content_type' => 'ENUM("pdf","doc","txt","html")', '$.content_id' => 'INT']);
        $vectorTable->getConnection()->begin_transaction();
        // Insert vectors with metadata
        $ids = [];
        $ids[] = $vectorTable->upsert(array_fill(0, $this->dimension, 0.1), ['content_type' => 'pdf', 'content_id' => 456, 'chunk_hash' => 'abc']);
        $ids[] = $vectorTable->upsert(array_fill(0, $this->dimension, 0.2), ['content_type' => 'pdf', 'content_id' => 123, 'chunk_hash' => null]);
        $ids[] = $vectorTable->upsert(array_fill(0, $this->dimension, 0.3), ['content_type' => 'html', 'content_id' => 456]);
        $ids[] = $vectorTable->upsert(array_fill(0, $this->dimension, 0.4), null); // null metadata
        // AND of equality - content_type='pdf' AND content_id=456 (should use indexes)
        $rows = $vectorTable->selectByMetadata(['$.content_type' => 'pdf', '$.content_id' => 456]);
        $this->assertCount(1, $rows);
        $this->assertEquals('pdf', $rows[0]['metadata']['content_type']);
        $this->assertEquals(456, $rows[0]['metadata']['content_id']);
        // Null equality semantics: JSON path => null matches:
        // 1) explicit JSON null for the path
        // 2) missing path in metadata object
        // 3) entire metadata column is NULL
        $rows = $vectorTable->selectByMetadata(['$.chunk_hash' => null]);
        $this->assertCount(3, $rows);
        $this->assertArrayHasKey('chunk_hash', $rows[0]['metadata']);
        $this->assertNull($rows[0]['metadata']['chunk_hash']);
        $vectorTable->getConnection()->rollback();
    }

    public function testSelectByMetadata_UsesExistingIndexesWithNewInstance(): void {
        // First create a table and add indexes
        $tableWithIdx = $this->makeTable('select_by_metadata_existing_idx', $this->dimension, ['$.content_type' => 'ENUM("pdf","doc","txt","html")', '$.content_id' => 'INT']);
        // Insert a few rows
        $tableWithIdx->upsert(array_fill(0, $this->dimension, 0.1), ['content_type' => 'pdf', 'content_id' => 111]);
        $tableWithIdx->upsert(array_fill(0, $this->dimension, 0.2), ['content_type' => 'html', 'content_id' => 222]);
        // Create a new instance pointing to the same underlying table WITHOUT calling initializeTables again
        $tableName = $tableWithIdx->getVectorTableName();
        $existingInstance = new \MHz\MysqlVector\VectorTable(self::$mysqli, str_replace('_vectors','',$tableName), $this->dimension);
        // Query using shorthand AND conditions (should detect and use existing indexes)
        $rows = $existingInstance->selectByMetadata(['$.content_type' => 'pdf', '$.content_id' => 111]);
        $this->assertCount(1, $rows);
        $this->assertEquals('pdf', $rows[0]['metadata']['content_type']);
        $this->assertEquals(111, $rows[0]['metadata']['content_id']);
    }

    public function testDetectPrimitiveTypeMappings(): void {
        $vt = new \MHz\MysqlVector\VectorTable(self::$mysqli, 'detect_primitive', 16);
        $ref = new \ReflectionMethod(\MHz\MysqlVector\VectorTable::class, 'detectPrimitiveType');
        $ref->setAccessible(true);

        // string-like
        $this->assertEquals('string', $ref->invoke($vt, 'char(10)'));
        $this->assertEquals('string', $ref->invoke($vt, ' VARCHAR(191) '));
        $this->assertEquals('string', $ref->invoke($vt, 'text'));
        $this->assertEquals('string', $ref->invoke($vt, 'tinytext'));
        $this->assertEquals('string', $ref->invoke($vt, 'mediumtext'));
        $this->assertEquals('string', $ref->invoke($vt, 'longtext'));
        $this->assertEquals('string', $ref->invoke($vt, "ENUM('A','B')"));
        $this->assertEquals('string', $ref->invoke($vt, 'set("x","y")'));
        $this->assertEquals('string', $ref->invoke($vt, 'date'));

        // integer-like
        $this->assertEquals('integer', $ref->invoke($vt, 'smallint'));
        $this->assertEquals('integer', $ref->invoke($vt, 'mediumint'));
        $this->assertEquals('integer', $ref->invoke($vt, 'int'));
        $this->assertEquals('integer', $ref->invoke($vt, 'integer'));
        $this->assertEquals('integer', $ref->invoke($vt, 'bigint'));
        $this->assertEquals('integer', $ref->invoke($vt, 'tinyint(2)'));
        $this->assertEquals('integer', $ref->invoke($vt, 'timestamp'));
        $this->assertEquals('integer', $ref->invoke($vt, 'datetime'));

        // float-like
        $this->assertEquals('float', $ref->invoke($vt, 'decimal(10,2)'));
        $this->assertEquals('float', $ref->invoke($vt, 'numeric(12,5)'));
        $this->assertEquals('float', $ref->invoke($vt, 'float'));
        $this->assertEquals('float', $ref->invoke($vt, 'double'));
        $this->assertEquals('float', $ref->invoke($vt, ' real '));

        // boolean
        $this->assertEquals('boolean', $ref->invoke($vt, 'tinyint(1)'));
    }

    public function testTypedMetadataIndexingAndSelectByMetadata(): void {
        $vt = $this->makeTable('typed_meta', $this->dimension, [
            '$.title' => 'VARCHAR(191)',
            '$.category' => 'ENUM("A","B","C")',
            '$.count' => 'INT',
            '$.timestamp' => 'BIGINT',
            '$.price' => 'DECIMAL(10,2)',
            '$.rating' => 'FLOAT',
            '$.active' => 'BOOLEAN'
        ]);
        $vt->getConnection()->begin_transaction();

        // Insert a few rows with typed metadata
        $vt->upsert(array_fill(0, $this->dimension, 0.01), [
            'title' => 'Doc 1', 'category' => 'A', 'count' => 10, 'timestamp' => 1700000000,
            'price' => 12.34, 'rating' => 4.5, 'active' => true
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.02), [
            'title' => 'Doc 2', 'category' => 'B', 'count' => 20, 'timestamp' => 1700000100,
            'price' => 99.99, 'rating' => 3.0, 'active' => false
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.03), [
            'title' => 'Doc 3', 'category' => 'C', 'count' => 30, 'timestamp' => 1700000200,
            'price' => 7.00, 'rating' => 2.25, 'active' => true
        ]);

        // Indexed string
        $r = $vt->selectByMetadata(['$.title' => 'Doc 1']);
        $this->assertCount(1, $r);
        $this->assertEquals('Doc 1', $r[0]['metadata']['title']);
        // Indexed enum (string)
        $r = $vt->selectByMetadata(['$.category' => 'B']);
        $this->assertCount(1, $r);
        $this->assertEquals('B', $r[0]['metadata']['category']);
        // Indexed integer
        $r = $vt->selectByMetadata(['$.count' => 20]);
        $this->assertCount(1, $r);
        $this->assertEquals(20, $r[0]['metadata']['count']);
        // Integer type validation should reject non-numeric string
        try {
            $vt->selectByMetadata(['$.count' => 'abc']);
            $this->fail('Expected InvalidArgumentException for non-numeric integer value');
        } catch (\InvalidArgumentException) { /* expected */ }
        // Indexed bigint
        $r = $vt->selectByMetadata(['$.timestamp' => 1700000100]);
        $this->assertCount(1, $r);
        // Indexed decimal (float primitive) – allow numeric string
        $r = $vt->selectByMetadata(['$.price' => '99.99']);
        $this->assertCount(1, $r);
        $this->assertEquals(99.99, (float)$r[0]['metadata']['price']);
        // Indexed float
        $r = $vt->selectByMetadata(['$.rating' => 3.0]);
        $this->assertCount(1, $r);
        // Indexed boolean stored as BOOLEAN (TINYINT(1)) – query with PHP booleans
        $r = $vt->selectByMetadata(['$.active' => true]);
        $this->assertCount(2, $r); // id1 and id3
        $r = $vt->selectByMetadata(['$.active' => false]);
        $this->assertCount(1, $r); // id2

        // getMetadataIndexMap returns typed entries with primitive_type
        $refMap = new \ReflectionMethod(\MHz\MysqlVector\VectorTable::class, 'getMetadataIndexMap');
        $refMap->setAccessible(true);
        $map = $refMap->invoke($vt);
        $this->assertEquals('string', $map['$.title']['primitive_type']);
        $this->assertEquals('string', $map['$.category']['primitive_type']);
        $this->assertEquals('integer', $map['$.count']['primitive_type']);
        $this->assertEquals('integer', $map['$.timestamp']['primitive_type']);
        $this->assertEquals('float', $map['$.price']['primitive_type']);
        $this->assertEquals('float', $map['$.rating']['primitive_type']);
        $this->assertEquals('boolean', $map['$.active']['primitive_type']);

        // Fallback (non-indexed) path should work with JSON typing
        $r = $vt->selectByMetadata(['$.non_indexed' => 'foo']);
        $this->assertIsArray($r); // no error; likely empty set

        $vt->getConnection()->rollback();
    }

    public function testSelectByMetadata_FallbackJsonExtractTyping(): void
    {
        $vt = $this->makeTable('fallback_meta', $this->dimension);
        $vt->getConnection()->begin_transaction();

        // Insert rows with non-indexed metadata
        $vt->upsert(array_fill(0, $this->dimension, 0.11), [
            'name' => 'Alice', 'score' => 42, 'price' => 12.5, 'active' => true, 'null_field' => null
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.12), [
            'name' => 'Bob', 'score' => 7, 'price' => 3.14, 'active' => false
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.13), [
            'name' => 'Carol', 'score' => 42, 'price' => 12.5
        ]); // missing null_field, active

        // String equality
        $r = $vt->selectByMetadata(['$.name' => 'Alice']);
        $this->assertCount(1, $r);
        // Integer equality
        $r = $vt->selectByMetadata(['$.score' => 42]);
        $this->assertCount(2, $r); // Alice and Carol
        // Float equality (ensure numeric literal used)
        $r = $vt->selectByMetadata(['$.price' => 12.5]);
        $this->assertCount(2, $r); // Alice and Carol
        // Boolean equality
        $r = $vt->selectByMetadata(['$.active' => true]);
        $this->assertCount(1, $r); // Alice
        $r = $vt->selectByMetadata(['$.active' => false]);
        $this->assertCount(1, $r); // Bob
        // Null semantics: matches explicit null, missing path, and metadata NULL (none here)
        $r = $vt->selectByMetadata(['$.null_field' => null]);
        $this->assertCount(3, $r); // Alice (explicit null) + Bob (missing) + Carol (missing)

        $vt->getConnection()->rollback();
    }

    public function testBatchInsert_EmptyInput_ReturnsEmptyAndNoChange(): void
    {
        $vectorTable = $this->makeTable('batch_empty_input', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();
        $before = $vectorTable->count();
        $vectorTable->batchInsert([]);
        $after = $vectorTable->count();
        $this->assertEquals($before, $after);
        $vectorTable->getConnection()->rollback();
    }

    public function testBatchInsert_VaryingMetadata_DynamicBatchingWorks(): void
    {
        $vectorTable = $this->makeTable('batch_varying_meta', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $count = 50;
        $items = [];
        for ($i = 0; $i < $count; $i++) {
            $v = $this->getRandomVectors(1, $this->dimension)[0];
            $metaLen = ($i % 5) * 2048; // up to 8KB metadata to vary payloads
            $meta = $metaLen > 0 ? ['m' => str_repeat('A', $metaLen)] : null;
            $items[] = ['vector' => $v, 'metadata' => $meta];
        }

        $vectorTable->batchInsert($items);
        $this->assertEquals($count, $vectorTable->count());

        $vectorTable->getConnection()->rollback();
    }

    public function testBatchInsert_SingleRowExceedsBudget_Throws(): void
    {
        // Query server max_allowed_packet and compute a single-row payload that exceeds 90% budget
        $res = self::$mysqli->query('SELECT @@max_allowed_packet');
        $this->assertNotFalse($res, 'Failed to query max_allowed_packet');
        $row = $res->fetch_row();
        $res->free();
        $this->assertIsArray($row);
        $maxPacket = (int)$row[0];

        $vectorTable = $this->makeTable('batch_exceed_row', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        $budget = (int) floor(0.9 * $maxPacket);
        $vecBytes = 4 * $this->dimension;
        $codeBytes = (int) ceil($this->dimension / 8);
        $codeHexChars = 2 * $codeBytes;
        $overshoot = 64 * 1024;
        $targetParamBytes = $budget + $overshoot;
        $emptyJsonOverhead = strlen(json_encode(['m' => '']));
        $valueLen = max(1, $targetParamBytes - $vecBytes - $codeHexChars - $emptyJsonOverhead);
        $metadata = ['m' => str_repeat('A', $valueLen)];

        $vector = $this->getRandomVectors(1, $this->dimension)[0];
        $items = [['vector' => $vector, 'metadata' => $metadata]];

        try {
            $vectorTable->batchInsert($items);
            $this->fail('Expected Exception due to single row exceeding safe packet size budget');
        } catch (\Exception $e) {
            $this->assertStringContainsString('exceeds safe packet size budget', $e->getMessage());
        } finally {
            $vectorTable->getConnection()->rollback();
        }
    }

    public function testBatchInsert_SingleRowAtBudget_Succeeds(): void
    {
        $res = self::$mysqli->query('SELECT @@max_allowed_packet');
        $this->assertNotFalse($res, 'Failed to query max_allowed_packet');
        $row = $res->fetch_row();
        $res->free();
        $this->assertIsArray($row);
        $maxPacket = (int)$row[0];

        $vt = $this->makeTable('batch_single_at_budget', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $budget = (int) floor(0.9 * $maxPacket);
        $valuesTemplate = "(?, UNHEX(?), ?)";
        $perRowSqlOverhead = strlen($valuesTemplate) + 1;
        $sqlHeader = "INSERT INTO `" . $vt->getVectorTableName() . "` (normalized_vector, binary_code, metadata) VALUES ";
        $baseSqlLen = strlen($sqlHeader) - 1;

        $vecBytes = 4 * $this->dimension;
        $codeBytes = (int) ceil($this->dimension / 8);
        $codeHexChars = 2 * $codeBytes;
        $perParamTypeBytes = 2; // COM_STMT_EXECUTE type+flag per param
        $typesArrayBytes = 3 * $perParamTypeBytes; // 3 params/row

        $emptyJsonOverhead = strlen(json_encode(['m' => '']));
        // Include initial NULL-bitmap byte for the first execute packet
        $targetRowParamBytes = max(1, $budget - $baseSqlLen - $perRowSqlOverhead - 1);
        $valueLen = max(1, $targetRowParamBytes - $vecBytes - $codeHexChars - $typesArrayBytes - $emptyJsonOverhead);
        $metadata = ['m' => str_repeat('A', $valueLen)];

        $vector = $this->getRandomVectors(1, $this->dimension)[0];
        $items = [['vector' => $vector, 'metadata' => $metadata]];

        try {
            $vt->batchInsert($items);
            $this->assertEquals(1, $vt->count());
        } finally {
            $vt->getConnection()->rollback();
        }
    }

    public function testBatchInsert_AtParameterLimit_Succeeds(): void
    {
        // With 3 params per row, 60,000 params => 20,000 rows in a single statement
        $rows = intdiv(60000, 3);
        $dimension = 1; // keep payload small to avoid hitting packet limits first
        $vt = $this->makeTable('batch_param_limit', $dimension);
        $vt->getConnection()->begin_transaction();

        $items = [];
        for ($i = 0; $i < $rows; $i++) {
            $items[] = ['vector' => [0.01], 'metadata' => null];
        }

        try {
            $vt->batchInsert($items);
            $this->assertEquals($rows, $vt->count());
        } finally {
            $vt->getConnection()->rollback();
        }
    }

    public function testBatchDelete_Basic(): void {
        $vt = $this->makeTable('batch_delete_basic', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $ids = [];
        foreach ($this->getRandomVectors(20, $this->dimension) as $v) {
            $ids[] = $vt->upsert($v);
        }
        $this->assertEquals(20, $vt->count());

        // Delete half in one call
        $toDelete = array_slice($ids, 0, 10);
        $vt->batchDelete($toDelete);
        $this->assertEquals(10, $vt->count());

        // Delete the rest
        $vt->batchDelete(array_slice($ids, 10));
        $this->assertEquals(0, $vt->count());

        $vt->getConnection()->rollback();
    }

    public function testBatchDelete_EmptyInput_NoOp(): void {
        $vt = $this->makeTable('batch_delete_empty', $this->dimension);
        $vt->getConnection()->begin_transaction();

        foreach ($this->getRandomVectors(5, $this->dimension) as $v) { $vt->upsert($v); }
        $before = $vt->count();
        $vt->batchDelete([]);
        $this->assertEquals($before, $vt->count());

        $vt->getConnection()->rollback();
    }

    public function testBatchDelete_ParameterLimitScale(): void {
        // Exercise chunking path with many ids but small vectors for speed
        $dimension = 1;
        $vt = $this->makeTable('batch_delete_many', $dimension);
        $vt->getConnection()->begin_transaction();

        $rows = intdiv(60000, 1) + 100; // > 60k rows to ensure multiple chunks
        $items = array_fill(0, $rows, ['vector' => [0.01]]);
        $vt->batchInsert($items);
        $this->assertEquals($rows, $vt->count());

        // Collect ids after insert and batch delete them
        $ids = array_column($vt->selectAll(), 'id');
        $vt->batchDelete($ids);
        $this->assertEquals(0, $vt->count());

        $vt->getConnection()->rollback();
    }

    public function testDeleteByMetadata_Indexed(): void
    {
        $vt = $this->makeTable('delete_meta_idx', $this->dimension, [
            '$.content_type' => 'ENUM("pdf","doc")',
            '$.content_id'   => 'INT',
            '$.price'        => 'DECIMAL(10,2)',
            '$.active'       => 'BOOLEAN',
        ]);
        $vt->getConnection()->begin_transaction();

        // Insert rows with typed metadata
        $vt->upsert(array_fill(0, $this->dimension, 0.01), [
            'content_type' => 'pdf', 'content_id' => 111, 'price' => 9.99, 'active' => true
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.02), [
            'content_type' => 'pdf', 'content_id' => 222, 'price' => 9.99, 'active' => false
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.03), [
            'content_type' => 'doc', 'content_id' => 111, 'price' => 19.99, 'active' => true
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.04), null); // NULL metadata

        $this->assertEquals(4, $vt->count());

        // Delete by indexed equality (enum + int)
        $deleted = $vt->deleteByMetadata(['$.content_type' => 'pdf', '$.content_id' => 111]);
        $this->assertEquals(1, $deleted);
        $this->assertEquals(3, $vt->count());

        // Delete by indexed decimal (allow numeric string)
        $deleted2 = $vt->deleteByMetadata(['$.price' => '9.99']);
        $this->assertEquals(1, $deleted2);
        $this->assertEquals(2, $vt->count());

        // Delete by NULL semantics: matches explicit null path, missing path, and metadata NULL
        $deleted3 = $vt->deleteByMetadata(['$.any_missing_or_null' => null]);
        $this->assertEquals(2, $deleted3); // removes id3 (missing path) and id4 (metadata NULL)
        $this->assertEquals(0, $vt->count());

        $vt->getConnection()->rollback();
    }

    public function testDeleteByMetadata_FallbackJsonExtractTyping(): void
    {
        $vt = $this->makeTable('delete_meta_fallback', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.11), [
            'name' => 'Alice', 'score' => 42, 'price' => 12.5, 'active' => true, 'null_field' => null
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.12), [
            'name' => 'Bob', 'score' => 7, 'price' => 3.14, 'active' => false
        ]);
        $vt->upsert(array_fill(0, $this->dimension, 0.13), [
            'name' => 'Carol', 'score' => 42, 'price' => 12.5
        ]);

        $this->assertEquals(3, $vt->count());

        // String equality
        $this->assertEquals(1, $vt->deleteByMetadata(['$.name' => 'Alice']));
        $this->assertEquals(2, $vt->count());

        // Integer equality
        $this->assertEquals(1, $vt->deleteByMetadata(['$.score' => 42])); // removes Carol
        $this->assertEquals(1, $vt->count());

        // Float equality
        $this->assertEquals(1, $vt->deleteByMetadata(['$.price' => 3.14])); // removes Bob
        $this->assertEquals(0, $vt->count());

        $vt->getConnection()->rollback();
    }

    public function testDeleteByMetadata_NullSemantics(): void
    {
        $vt = $this->makeTable('delete_meta_nulls', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.21), ['chunk' => null]); // explicit null
        $vt->upsert(array_fill(0, $this->dimension, 0.22), ['other' => 'x']); // missing path
        $vt->upsert(array_fill(0, $this->dimension, 0.23), null);            // metadata NULL
        $vt->upsert(array_fill(0, $this->dimension, 0.24), ['chunk' => 'value']); // should remain

        $this->assertEquals(4, $vt->count());

        $deleted = $vt->deleteByMetadata(['$.chunk' => null]);
        $this->assertEquals(3, $deleted); // id1, id2, id3
        $this->assertEquals(1, $vt->count());

        $rows = $vt->selectAll();
        $this->assertCount(1, $rows);
        $this->assertEquals('value', $rows[0]['metadata']['chunk']);

        $vt->getConnection()->rollback();
    }

    public function testDeleteByMetadata_EmptyConditions_Throws(): void
    {
        $vt = $this->makeTable('delete_meta_empty', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.31), ['a' => 1]);
        $this->assertEquals(1, $vt->count());

        try {
            $vt->deleteByMetadata([]);
            $this->fail('Expected InvalidArgumentException for empty deleteByMetadata conditions');
        } catch (\InvalidArgumentException) {
            $this->assertTrue(true);
        }

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_IndexedBasic(): void
    {
        $vt = $this->makeTable('search_meta_idx', $this->dimension, [
            '$.content_type' => 'ENUM("pdf","doc","html")',
            '$.content_id'   => 'INT'
        ]);
        $vt->getConnection()->begin_transaction();

        // Insert rows
        $id1 = $vt->upsert(array_fill(0, $this->dimension, 0.50), ['content_type' => 'pdf',  'content_id' => 123]);
        $vt->upsert(array_fill(0, $this->dimension, 0.40), ['content_type' => 'pdf',  'content_id' => 456]);
        $vt->upsert(array_fill(0, $this->dimension, 0.30), ['content_type' => 'html', 'content_id' => 123]);

        // Query vector close to id1
        $q = array_fill(0, $this->dimension, 0.50);

        // Single indexed condition
        $results = $vt->search($q, ['$.content_type' => 'pdf'], 10);
        $this->assertNotEmpty($results);
        foreach ($results as $r) {
            $this->assertEquals('pdf', $r['metadata']['content_type']);
        }

        // AND of indexed conditions narrows to content_type=pdf AND content_id=123
        $resultsAnd = $vt->search($q, ['$.content_type' => 'pdf', '$.content_id' => 123], 10);
        $this->assertNotEmpty($resultsAnd);
        $this->assertEquals($id1, $resultsAnd[0]['id']);
        $this->assertEquals('pdf', $resultsAnd[0]['metadata']['content_type']);
        $this->assertEquals(123, $resultsAnd[0]['metadata']['content_id']);

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_NonIndexedBasic(): void
    {
        $vt = $this->makeTable('search_meta_fallback', $this->dimension);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.11), ['name' => 'Alice', 'score' => 42]);
        $vt->upsert(array_fill(0, $this->dimension, 0.12), ['name' => 'Bob',   'score' => 7]);
        $vt->upsert(array_fill(0, $this->dimension, 0.13), ['name' => 'Carol', 'score' => 42]);

        $q = array_fill(0, $this->dimension, 0.12);
        $results = $vt->search($q, ['$.score' => 42], 10);
        $this->assertNotEmpty($results);
        foreach ($results as $r) {
            $this->assertEquals(42, $r['metadata']['score']);
        }

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_NullSemantics(): void
    {
        $vt = $this->makeTable('search_meta_nulls', $this->dimension);
        $vt->getConnection()->begin_transaction();

        // explicit JSON null
        $id1 = $vt->upsert(array_fill(0, $this->dimension, 0.21), ['chunk_hash' => null]);
        // missing path
        $id2 = $vt->upsert(array_fill(0, $this->dimension, 0.22), ['x' => 'y']);
        // metadata NULL
        $id3 = $vt->upsert(array_fill(0, $this->dimension, 0.23), null);
        // control (non-null value)
        $id4 = $vt->upsert(array_fill(0, $this->dimension, 0.24), ['chunk_hash' => 'value']);

        $q = array_fill(0, $this->dimension, 0.22);
        $results = $vt->search($q, ['$.chunk_hash' => null], 10);
        $this->assertNotEmpty($results);
        $ids = array_column($results, 'id');
        $this->assertContains($id1, $ids);
        $this->assertContains($id2, $ids);
        $this->assertContains($id3, $ids);
        $this->assertNotContains($id4, $ids);

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_EmptyAndNullConditions(): void
    {
        $vt = $this->makeTable('search_meta_empty', $this->dimension);
        $vt->getConnection()->begin_transaction();

        for ($i = 0; $i < 5; $i++) { $vt->upsert(array_fill(0, $this->dimension, 0.3 + 0.01*$i), ['a' => $i]); }
        $q = array_fill(0, $this->dimension, 0.31);

        $r1 = $vt->search($q); // default
        $r2 = $vt->search($q, null);
        $r3 = $vt->search($q, []);

        $this->assertEquals(array_column($r1, 'id'), array_column($r2, 'id'));
        $this->assertEquals(array_column($r1, 'id'), array_column($r3, 'id'));

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_ResultAccuracyOnSubset(): void
    {
        $vt = $this->makeTable('search_meta_accuracy', $this->dimension, [
            '$.content_type' => 'ENUM("pdf","doc")'
        ]);
        $vt->getConnection()->begin_transaction();

        $ids = [];
        $ids[] = $vt->upsert(array_fill(0, $this->dimension, 0.50), ['content_type' => 'pdf']);
        $ids[] = $vt->upsert(array_fill(0, $this->dimension, 0.49), ['content_type' => 'pdf']);
        $ids[] = $vt->upsert(array_fill(0, $this->dimension, 0.10), ['content_type' => 'doc']);
        $ids[] = $vt->upsert(array_fill(0, $this->dimension, 0.05), ['content_type' => 'doc']);

        $q = array_fill(0, $this->dimension, 0.50);
        // Unfiltered order
        $all = $vt->search($q, null, 10, 20);
        $pdfSet = array_filter($all, fn($r) => ($r['metadata']['content_type'] ?? null) === 'pdf');
        $pdfIdsFromAll = array_values(array_column($pdfSet, 'id'));

        // Filtered order
        $filtered = $vt->search($q, ['$.content_type' => 'pdf'], 10, 20);
        $filteredIds = array_column($filtered, 'id');

        $this->assertEquals($pdfIdsFromAll, $filteredIds, 'Filtered search order should match subset order from unfiltered search');

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_CandidateMultiplierInteraction(): void
    {
        $vt = $this->makeTable('search_meta_multiplier', $this->dimension, [
            '$.category' => 'ENUM("A","B")'
        ]);
        $vt->getConnection()->begin_transaction();

        for ($i = 0; $i < 20; $i++) {
            $val = ($i % 2 === 0) ? 'A' : 'B';
            $vt->upsert(array_fill(0, $this->dimension, 0.2 + 0.01*$i), ['category' => $val]);
        }
        $q = array_fill(0, $this->dimension, 0.25);

        // Default adaptive
        $rDefault = $vt->search($q, ['$.category' => 'A'], 5);
        $this->assertNotEmpty($rDefault);

        // Minimal multiplier
        $rMin = $vt->search($q, ['$.category' => 'A'], 5, 1);
        $this->assertNotEmpty($rMin);

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_MixedIndexedAndFallback(): void
    {
        $vt = $this->makeTable('search_meta_mixed', $this->dimension, [
            '$.category' => 'ENUM("A","B")'
        ]);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.30), ['category' => 'A', 'score' => 42]);
        $vt->upsert(array_fill(0, $this->dimension, 0.20), ['category' => 'A', 'score' => 7]);
        $vt->upsert(array_fill(0, $this->dimension, 0.10), ['category' => 'B', 'score' => 42]);

        $q = array_fill(0, $this->dimension, 0.30);
        $r = $vt->search($q, ['$.category' => 'A', '$.score' => 42], 10);
        $this->assertNotEmpty($r);
        $this->assertEquals('A', $r[0]['metadata']['category']);
        $this->assertEquals(42, $r[0]['metadata']['score']);

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_InvalidJsonPath_Throws(): void
    {
        $vt = $this->makeTable('search_meta_badpath', $this->dimension);
        $vt->getConnection()->begin_transaction();
        $vt->upsert(array_fill(0, $this->dimension, 0.10), ['content_type' => 'pdf']);
        $q = array_fill(0, $this->dimension, 0.10);

        try {
            $vt->search($q, ['content_type' => 'pdf'], 10); // missing '$.' prefix
            $this->fail('Expected InvalidArgumentException for invalid JSON path');
        } catch (\InvalidArgumentException) {
            $this->assertTrue(true);
        }

        $vt->getConnection()->rollback();
    }

    public function testSearch_MetadataPreFilter_NoMatches_ReturnsEmpty(): void
    {
        $vt = $this->makeTable('search_meta_nomatch', $this->dimension, [
            '$.content_type' => 'ENUM("pdf","doc")'
        ]);
        $vt->getConnection()->begin_transaction();

        $vt->upsert(array_fill(0, $this->dimension, 0.50), ['content_type' => 'pdf']);
        $q = array_fill(0, $this->dimension, 0.50);

        $r = $vt->search($q, ['$.content_type' => 'doc', '$.nonexistent' => 1], 10);
        $this->assertIsArray($r);
        $this->assertCount(0, $r);

        $vt->getConnection()->rollback();
    }
}
