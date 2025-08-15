<?php

namespace MHz\MysqlVector;

class VectorTable
{
    private string $name;
    private int $dimension;
    private string $engine;
    private \mysqli $mysqli;

    // Maximum supported vector dimensions, currently limited by VARBINARY storage
    // `normalized_vector` column uses VARBINARY(4 * dimension); VARBINARY max length in MySQL is 65,535 bytes
    // maximum supported dimensions for float32 storage = floor(65535 bytes / 4 bytes per float32) = 16383
    public const MAX_DIMENSIONS = 16383;

    /**
     * Instantiate a new VectorTable object.
     * @param \mysqli $mysqli The mysqli connection
     * @param string $name Name of the table.
     * @param int $dimension Dimension of the vectors.
     * @param string $engine The storage engine to use for the tables
     * @throws \InvalidArgumentException If dimension exceeds maximum supported value
     */
    public function __construct(\mysqli $mysqli, string $name, int $dimension = 384, string $engine = 'InnoDB')
    {
        if ($dimension <= 0) {
            throw new \InvalidArgumentException("Dimension must be a positive integer, got $dimension");
        }

        if ($dimension > self::MAX_DIMENSIONS) {
            throw new \InvalidArgumentException("Maximum supported dimension is " . self::MAX_DIMENSIONS . ", got $dimension");
        }

        $this->mysqli = $mysqli;
        $this->name = $name;
        $this->dimension = $dimension;
        $this->engine = $engine;
    }

    /**
     * Escape MySQL identifier using backticks
     *
     * @param string $identifier The identifier to escape
     * @return string The escaped identifier
     */
    private function escapeIdentifier(string $identifier): string
    {
        // For identifiers, escape backticks by doubling them, then wrap in backticks
        $escaped = str_replace('`', '``', $identifier);
        return "`$escaped`";
    }

    public function getVectorTableName(): string
    {
        return $this->name . '_vectors';
    }

    public function getDimension(): int
    {
        return $this->dimension;
    }

    /**
     * Convert an n-dimensional vector into an n-bit binary code using optimized chunking
     * @param array $vector Input vector
     * @return string Hexadecimal representation of binary quantized vector
     */
    public function vectorToHex(array $vector): string {
        $bytes = [];
        $chunks = array_chunk($vector, 8);

        foreach ($chunks as $chunk) {
            $byte = 0;
            foreach ($chunk as $i => $val) {
                if ($val > 0) {
                    $byte |= (1 << $i);
                }
            }
            $bytes[] = $byte;
        }

        return bin2hex(pack('C*', ...$bytes));
    }

    /**
     * Encode a PHP float vector to a little-endian float32 binary blob.
     * Uses pack('g', ...) to enforce LE regardless of platform endianness.
     */
    public function vectorToBlob(array $vector): string {
        $bin = '';
        foreach ($vector as $v) {
            $bin .= pack('g', (float)$v);
        }
        return $bin;
    }

    /**
     * Decode a little-endian float32 binary blob to a PHP float array.
     * Uses unpack('g*', ...) for single-pass decoding.
     */
    public function blobToVector(string $blob): array {
        // array_values to reindex from 0
        return array_values(unpack('g*', $blob));
    }

    /**
     * Initialize the tables for this VectorTable instance
     * Fails if tables have already been created
     * @return void
     * @throws \Exception If the tables could not be created (e.g., table already exists)
     */
    public function initializeTables(): void
    {
        // Build all SQL statements for single multi-query execution with proper escaping
        $binaryCodeLengthInBytes = ceil($this->dimension / 8);
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $engine = $this->escapeIdentifier($this->engine);

        $normalizedVectorLengthInBytes = 4 * $this->dimension; // float32 per component

        $queries = "
            CREATE TABLE {$escapedVectorTableName} (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                normalized_vector VARBINARY({$normalizedVectorLengthInBytes}),
                binary_code VARBINARY({$binaryCodeLengthInBytes})
            ) ENGINE={$engine};
        ";

        if (!$this->mysqli->multi_query($queries)) {
            throw new \Exception("Failed to initialize tables: " . $this->mysqli->error);
        }

        // Clear all results from multi-query
        do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());
    }

    /**
     * Create the tables required for storing vectors.
     * Fails if already initialized (table exists)
     * @return void
     * @throws \Exception If the tables could not be created
     */
    public function initialize(): void
    {
        // Initialize tables (instance-specific)
        $this->initializeTables();
    }

    /**
     * Compute the dot product between two equal-length numeric arrays using
     * a manual loop with x32 unrolling for performance on large vectors.
     */
    private function dot(array $a, array $b): float
    {
        $n = $this->dimension; // both arrays are validated to match this dimension
        $sum = 0.0;
        $i = 0;
        $limit = $n - ($n % 32);

        for (; $i < $limit; $i += 32) {
            $sum += ($a[$i] * $b[$i])
                  + ($a[$i + 1] * $b[$i + 1])
                  + ($a[$i + 2] * $b[$i + 2])
                  + ($a[$i + 3] * $b[$i + 3])
                  + ($a[$i + 4] * $b[$i + 4])
                  + ($a[$i + 5] * $b[$i + 5])
                  + ($a[$i + 6] * $b[$i + 6])
                  + ($a[$i + 7] * $b[$i + 7])
                  + ($a[$i + 8] * $b[$i + 8])
                  + ($a[$i + 9] * $b[$i + 9])
                  + ($a[$i + 10] * $b[$i + 10])
                  + ($a[$i + 11] * $b[$i + 11])
                  + ($a[$i + 12] * $b[$i + 12])
                  + ($a[$i + 13] * $b[$i + 13])
                  + ($a[$i + 14] * $b[$i + 14])
                  + ($a[$i + 15] * $b[$i + 15])
                  + ($a[$i + 16] * $b[$i + 16])
                  + ($a[$i + 17] * $b[$i + 17])
                  + ($a[$i + 18] * $b[$i + 18])
                  + ($a[$i + 19] * $b[$i + 19])
                  + ($a[$i + 20] * $b[$i + 20])
                  + ($a[$i + 21] * $b[$i + 21])
                  + ($a[$i + 22] * $b[$i + 22])
                  + ($a[$i + 23] * $b[$i + 23])
                  + ($a[$i + 24] * $b[$i + 24])
                  + ($a[$i + 25] * $b[$i + 25])
                  + ($a[$i + 26] * $b[$i + 26])
                  + ($a[$i + 27] * $b[$i + 27])
                  + ($a[$i + 28] * $b[$i + 28])
                  + ($a[$i + 29] * $b[$i + 29])
                  + ($a[$i + 30] * $b[$i + 30])
                  + ($a[$i + 31] * $b[$i + 31]);
        }

        for (; $i < $n; $i++) {
            $sum += $a[$i] * $b[$i];
        }

        return $sum;
    }

    /**
     * Clean up tables for this VectorTable instance
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitializeTables(): void
    {
        // Drop table with proper escaping
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());

        $queries = "DROP TABLE IF EXISTS {$escapedVectorTableName};";

        if (!$this->mysqli->multi_query($queries)) {
            throw new \Exception("Failed to drop tables: " . $this->mysqli->error);
        }

        // Clear all results from multi-query
        do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());
    }

    /**
     * Complete cleanup of tables.
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitialize(): void
    {
        // Clean up tables
        $this->deinitializeTables();
    }

    /**
     * Compute the cosine similarity between two vectors
     *
     * This method normalizes both input vectors and then computes their dot product,
     * which equals the cosine similarity for normalized vectors.
     *
     * @param array $v1 The first vector
     * @param array $v2 The second vector
     * @return float|null The cosine similarity between the two vectors [-1, 1], or null for invalid inputs
     * @throws \Exception
     */
    public function cosim(array $v1, array $v2): ?float
    {
        // Validate vector dimensions match
        if (count($v1) !== count($v2)) {
            throw new \InvalidArgumentException("Vector dimensions must match");
        }

        if (count($v1) !== $this->dimension) {
            throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
        }

        // Normalize both vectors before computing dot product (equals cosine similarity)
        $normalizedV1 = $this->normalize($v1);
        $normalizedV2 = $this->normalize($v2);

        return $this->dot($normalizedV1, $normalizedV2);
    }

    /**
     * Insert or update a vector
     * @param array $vector The vector to insert or update
     * @param int|null $id Optional ID of the vector to update
     * @return int The ID of the inserted or updated vector
     * @throws \Exception If the vector could not be inserted or updated
     */
    public function upsert(array $vector, ?int $id = null): int
    {
        // Validate vector dimension
        if (count($vector) !== $this->dimension) {
            throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
        }

        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());

        $insertQuery = empty($id) ?
            "INSERT INTO {$escapedTableName} (normalized_vector, binary_code) VALUES (?, UNHEX(?))" :
            "UPDATE {$escapedTableName} SET normalized_vector = ?, binary_code = UNHEX(?) WHERE id = ?";

        $statement = $this->mysqli->prepare($insertQuery);
        if(!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        $normalizedVectorBlob = $this->vectorToBlob($normalizedVector);

        if(empty($id)) {
            $statement->bind_param('ss', $normalizedVectorBlob, $binaryCode);
        } else {
            $statement->bind_param('ssi', $normalizedVectorBlob, $binaryCode, $id);
        }

        $success = $statement->execute();
        if(!$success) {
            throw new \Exception($statement->error);
        }

        $id = $statement->insert_id;
        $statement->close();

        return $id;
    }

    /**
     * Insert multiple vectors in a single query
     * @param array $vectorArray Array of vectors to insert
     * @return array Array of ids of the inserted vectors
     * @throws \Exception
     */
    public function batchInsert(array $vectorArray): array {
        $ids = [];

        $this->mysqli->begin_transaction();

        try {
            $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
            $statement = $this->mysqli->prepare("INSERT INTO {$escapedTableName} (normalized_vector, binary_code) VALUES (?, UNHEX(?))");
            if(!$statement) {
                throw new \Exception("Prepare failed: " . $this->mysqli->error);
            }

            foreach ($vectorArray as $vector) {
                // Validate vector dimension
                if (count($vector) !== $this->dimension) {
                    throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
                }

                $normalizedVector = $this->normalize($vector);
                $binaryCode = $this->vectorToHex($normalizedVector);
                $normalizedVectorBlob = $this->vectorToBlob($normalizedVector);

                $statement->bind_param('ss', $normalizedVectorBlob, $binaryCode);

                if (!$statement->execute()) {
                    throw new \Exception("Execute failed: " . $statement->error);
                }

                $ids[] = $statement->insert_id;
            }

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        } finally {
            if (isset($statement) && $statement) {
                $statement->close();
            }
        }

        return $ids;
    }

    /**
     * Select one or more vectors by id
     * @param \mysqli $mysqli The mysqli connection
     * @param array $ids The ids of the vectors to select
     * @return array Array of vectors
     */
    public function select(array $ids): array {

        $placeholders = implode(', ', array_fill(0, count($ids), '?'));
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM {$escapedVectorTableName} WHERE id IN ({$placeholders})");
        $types = str_repeat('i', count($ids));

        $refs = [];
        foreach ($ids as $key => &$id) {
            $refs[$key] = &$id;
        }
        unset($id);

        call_user_func_array([$statement, 'bind_param'], array_merge([$types], $refs));
        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVectorBlob, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => $this->blobToVector($normalizedVectorBlob),
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    public function selectAll(): array {

        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM {$escapedVectorTableName}");

        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVectorBlob, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => $this->blobToVector($normalizedVectorBlob),
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    /**
     * Returns the number of vectors stored in the database
     * @return int The number of vectors
     */
    public function count(): int {
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT COUNT(id) FROM {$escapedVectorTableName}");
        $statement->execute();
        $statement->bind_result($count);
        $statement->fetch();
        $statement->close();
        return $count;
    }

    /**
     * Calculate the Euclidean magnitude (L2 norm) of a vector
     *
     * @param array $vector Input vector
     * @return float The magnitude ||v|| = sqrt(v₁² + v₂² + ... + vₙ²)
     */
    private function getMagnitude(array $vector): float
    {
        $sumOfSquares = 0.0;
        foreach ($vector as $value) {
            $sumOfSquares += $value * $value;
        }
        return sqrt($sumOfSquares);
    }

    /**
     * Find vectors most similar to the given query vector using two-stage search
     *
     * Uses a two-stage algorithm for efficient similarity search:
     * 1. Stage 1: Binary quantization with Hamming distance for fast filtering
     * 2. Stage 2: Precise cosine similarity re-ranking of candidates
     *
     * @param array $vector Query vector to search for
     * @param int $n Maximum number of results to return (default: 10)
     * @return array Array of results, each containing:
     *               - 'id': Vector ID
     *               - 'similarity': Cosine similarity [-1, 1]
     * @throws \Exception If database operations fail or invalid input
     */
    public function search(array $vector, int $n = 10): array
    {
        // Input validation
        if (empty($vector)) {
            throw new \InvalidArgumentException("Search vector cannot be empty");
        }

        if (count($vector) !== $this->dimension) {
            throw new \InvalidArgumentException("Search vector dimension must match table dimension: {$this->dimension}");
        }

        if ($n <= 0) {
            throw new \InvalidArgumentException("Number of results must be positive");
        }

        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
        $queryVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($queryVector);

        // Stage 1: fetch top-N candidates by Hamming distance with their normalized vectors
        $sql = "
        SELECT id, normalized_vector
        FROM {$escapedTableName}
        ORDER BY BIT_COUNT(binary_code ^ UNHEX(?))
        LIMIT ?";

        $statement = $this->mysqli->prepare($sql);
        if (!$statement) {
            throw new \Exception("Failed to prepare search query: " . $this->mysqli->error);
        }

        $statement->bind_param('si', $binaryCode, $n);
        $statement->execute();
        $statement->bind_result($id, $normalizedVectorBlob);

        $candidates = [];
        while ($statement->fetch()) {
            $candidates[] = [
                'id' => $id,
                'normalized_vector' => $this->blobToVector($normalizedVectorBlob)
            ];
        }
        $statement->close();

        // Stage 2: PHP-side re-ranking using dot product
        $results = [];
        foreach ($candidates as $row) {
            $sim = $this->dot($row['normalized_vector'], $queryVector);
            $results[] = [
                'id' => $row['id'],
                'similarity' => $sim
            ];
        }

        // Sort by similarity desc and return top N
        usort($results, static function($a, $b) {
            if ($a['similarity'] === $b['similarity']) return 0;
            return ($a['similarity'] < $b['similarity']) ? 1 : -1;
        });

        if (count($results) > $n) {
            $results = array_slice($results, 0, $n);
        }

        return $results;
    }

    /**
     * Normalize a vector to unit length (L2 normalization)
     *
     * Converts a vector to unit length while preserving direction.
     * For zero vectors, uses epsilon to avoid division by zero.
     *
     * @param array $vector Input vector to normalize
     * @return array Normalized vector with magnitude ≈ 1.0
     */
    public function normalize(array $vector): array
    {
        // Calculate magnitude
        $magnitude = $this->getMagnitude($vector);

        // Small value to use for zero vectors (default: 1e-12)
        $epsilon = floatval(1e-12);

        // Handle zero and near-zero vectors with epsilon to avoid division by very small numbers
        if (abs($magnitude) < $epsilon) {
            $magnitude = $epsilon;
        }

        // Normalize: v_normalized = v / ||v||
        return array_map(fn($component) => $component / $magnitude, $vector);
    }

    /**
     * Remove a vector from the database
     * @param int $id The id of the vector to remove
     * @return void
     * @throws \Exception
     */
    public function delete(int $id): void {
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("DELETE FROM {$escapedVectorTableName} WHERE id = ?");
        $statement->bind_param('i', $id);
        $success = $statement->execute();
        if(!$success) {
            throw new \Exception($statement->error);
        }
        $statement->close();
    }

    public function getConnection(): \mysqli {
        return $this->mysqli;
    }
}