<?php

namespace MHz\MysqlVector;

class VectorTable
{
    private string $name;
    private int $dimension;
    private string $engine;
    private \mysqli $mysqli;

    const SQL_DOT_PRODUCT_FUNCTION = "
        CREATE FUNCTION MV_DOT_PRODUCT(v1 JSON, v2 JSON)
        RETURNS FLOAT
        DETERMINISTIC
        READS SQL DATA
        BEGIN
            DECLARE sim FLOAT DEFAULT 0;
            DECLARE i INT DEFAULT 0;
            DECLARE len INT DEFAULT JSON_LENGTH(v1);

            IF JSON_LENGTH(v1) != JSON_LENGTH(v2) THEN
                RETURN NULL;
            END IF;

            WHILE i < len DO
                SET sim = sim + (JSON_EXTRACT(v1, CONCAT('$[', i, ']')) * JSON_EXTRACT(v2, CONCAT('$[', i, ']')));
                SET i = i + 1;
            END WHILE;

            RETURN sim;
        END";

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
        // Maximum dimensions limited by InnoDB prefix index limit of 3072 bytes
        // 3072 bytes * 8 bits/byte = 24,576 dimensions maximum
        // Using a conservative limit to account for MySQL configuration variations
        $maxDimensions = 24000;

        if ($dimension <= 0) {
            throw new \InvalidArgumentException("Dimension must be a positive integer, got: $dimension");
        }

        if ($dimension > $maxDimensions) {
            $maxBytes = ceil($dimension / 8);
            throw new \InvalidArgumentException(
                "Dimension $dimension requires $maxBytes bytes for binary indexing, " .
                "which exceeds MySQL's InnoDB prefix index limit of 3072 bytes. " .
                "Maximum supported dimensions: $maxDimensions"
            );
        }

        $this->mysqli = $mysqli;
        $this->name = $name;
        $this->dimension = $dimension;
        $this->engine = $engine;
    }

    public function getVectorTableName(): string
    {
        return sprintf('%s_vectors', $this->name);
    }

    public function getDimension(): int
    {
        return $this->dimension;
    }

    protected function getCreateStatements(bool $ifNotExists = true): array {
        $binaryCodeLengthInBytes = ceil($this->dimension / 8);

        $vectorsQuery =
            "CREATE TABLE %s %s (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                normalized_vector JSON,
                binary_code VARBINARY(%d),
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=%s;";
        $vectorsQuery = sprintf($vectorsQuery, $ifNotExists ? 'IF NOT EXISTS' : '', $this->getVectorTableName(), $binaryCodeLengthInBytes, $this->engine);

        return [$vectorsQuery];
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
     * Initialize only the tables for this VectorTable instance
     * @param bool $ifNotExists Whether to use IF NOT EXISTS in the CREATE TABLE statements
     * @return void
     * @throws \Exception If the tables could not be created
     */
    public function initializeTables(bool $ifNotExists = true): void
    {
        $this->mysqli->begin_transaction();
        try {
            foreach ($this->getCreateStatements($ifNotExists) as $statement) {
                $success = $this->mysqli->query($statement);
                if (!$success) {
                    throw new \Exception($this->mysqli->error);
                }
            }

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Initialize global MySQL functions (should be called once per database)
     * @param \mysqli $mysqli Database connection
     * @return void
     * @throws \Exception If the functions could not be created
     */
    public static function initializeFunctions(\mysqli $mysqli): void
    {
        $mysqli->begin_transaction();
        try {
            // Add MV_DOT_PRODUCT function
            $mysqli->query("DROP FUNCTION IF EXISTS MV_DOT_PRODUCT");
            $res = $mysqli->query(self::SQL_DOT_PRODUCT_FUNCTION);

            if (!$res) {
                throw new \Exception("Failed to create MV_DOT_PRODUCT function: " . $mysqli->error);
            }

            $mysqli->commit();
        } catch (\Exception $e) {
            $mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Create the tables and functions required for storing vectors
     * @param bool $ifNotExists Whether to use IF NOT EXISTS in the CREATE TABLE statements
     * @return void
     * @throws \Exception If the tables or functions could not be created
     */
    public function initialize(bool $ifNotExists = true): void
    {
        // Initialize functions first (global)
        self::initializeFunctions($this->mysqli);

        // Then initialize tables (instance-specific)
        $this->initializeTables($ifNotExists);
    }

    /**
     * Clean up tables for this VectorTable instance
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitializeTables(): void
    {
        $tableName = $this->getVectorTableName();

        $this->mysqli->begin_transaction();
        try {
            // Drop the main vector table
            $this->mysqli->query("DROP TABLE IF EXISTS " . $tableName);

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Clean up global MySQL functions
     * @param \mysqli $mysqli Database connection
     * @return void
     * @throws \Exception If cleanup fails
     */
    public static function deinitializeFunctions(\mysqli $mysqli): void
    {
        $mysqli->begin_transaction();
        try {
            // Drop global functions
            $mysqli->query("DROP FUNCTION IF EXISTS MV_DOT_PRODUCT");

            $mysqli->commit();
        } catch (\Exception $e) {
            $mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Complete cleanup of tables and functions
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitialize(): void
    {
        // Clean up tables first
        $this->deinitializeTables();

        // Then clean up functions (global)
        self::deinitializeFunctions($this->mysqli);
    }

    /**
     * Compute the cosine similarity between two vectors
     *
     * This method normalizes both input vectors and then computes their dot product,
     * which equals the cosine similarity for normalized vectors.
     *
     * @param array $v1 The first vector
     * @param array $v2 The second vector
     * @return float The cosine similarity between the two vectors [-1, 1]
     * @throws \Exception
     */
    public function cosim(array $v1, array $v2): float
    {
        $statement = $this->mysqli->prepare("SELECT MV_DOT_PRODUCT(?, ?)");

        if(!$statement) {
            throw new \Exception("Failed to prepare dot product query: " . $this->mysqli->error);
        }

        // Normalize both vectors before computing dot product (which equals cosine similarity)
        $normalizedV1 = json_encode($this->normalize($v1));
        $normalizedV2 = json_encode($this->normalize($v2));

        $statement->bind_param('ss', $normalizedV1, $normalizedV2);
        $statement->execute();
        $statement->bind_result($similarity);
        $statement->fetch();
        $statement->close();

        return $similarity;
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
        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $tableName = $this->getVectorTableName();

        $insertQuery = empty($id) ?
            "INSERT INTO $tableName (normalized_vector, binary_code) VALUES (?, UNHEX(?))" :
            "UPDATE $tableName SET normalized_vector = ?, binary_code = UNHEX(?) WHERE id = $id";

        $statement = $this->mysqli->prepare($insertQuery);
        if(!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $normalizedVector = json_encode($normalizedVector);

        $statement->bind_param('ss', $normalizedVector, $binaryCode);

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
        $tableName = $this->getVectorTableName();

        $statement = $this->getConnection()->prepare("INSERT INTO $tableName (normalized_vector, binary_code) VALUES (?, UNHEX(?))");
        if(!$statement) {
            throw new \Exception("Prepare failed: " . $this->getConnection()->error);
        }

        $ids = [];
        $this->getConnection()->begin_transaction();
        try {
            foreach ($vectorArray as $vector) {
                $normalizedVector = $this->normalize($vector);
                $binaryCode = $this->vectorToHex($normalizedVector);
                $normalizedVectorJson = json_encode($normalizedVector);

                $statement->bind_param('ss', $normalizedVectorJson, $binaryCode);

                if (!$statement->execute()) {
                    throw new \Exception("Execute failed: " . $statement->error);
                }

                $ids[] = $statement->insert_id;
            }

            $this->getConnection()->commit();
        } catch (\Exception $e) {
            $this->getConnection()->rollback();
            throw $e;
        } finally {
            $statement->close();
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
        $tableName = $this->getVectorTableName();

        $placeholders = implode(', ', array_fill(0, count($ids), '?'));
        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM $tableName WHERE id IN ($placeholders)");
        $types = str_repeat('i', count($ids));

        $refs = [];
        foreach ($ids as $key => $id) {
            $refs[$key] = &$ids[$key];
        }

        call_user_func_array([$statement, 'bind_param'], array_merge([$types], $refs));
        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVector, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => json_decode($normalizedVector, true),
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    public function selectAll(): array {
        $tableName = $this->getVectorTableName();

        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM $tableName");

        if (!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVector, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => json_decode($normalizedVector, true),
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
        $tableName = $this->getVectorTableName();
        $statement = $this->mysqli->prepare("SELECT COUNT(id) FROM $tableName");
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
     *               - 'vector': Original vector
     *               - 'normalized_vector': L2-normalized vector
     *               - 'similarity': Cosine similarity [-1, 1]
     * @throws \Exception If database operations fail or invalid input
     */
    public function search(array $vector, int $n = 10): array
    {
        // Input validation
        if (empty($vector)) {
            throw new \InvalidArgumentException("Search vector cannot be empty");
        }

        if ($n <= 0) {
            throw new \InvalidArgumentException("Number of results must be positive");
        }
        $tableName = $this->getVectorTableName();
        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);

        // Initial search using binary codes
        $statement = $this->mysqli->prepare("SELECT id, BIT_COUNT(binary_code ^ UNHEX(?)) AS hamming_distance FROM $tableName ORDER BY hamming_distance LIMIT $n");

        if(!$statement) {
            throw new \Exception("Failed to prepare Hamming distance query: " . $this->mysqli->error);
        }

        $statement->bind_param('s', $binaryCode);
        $statement->execute();
        $statement->bind_result($vectorId, $hd);

        $candidates = [];
        while ($statement->fetch()) {
            $candidates[] = $vectorId;
        }
        $statement->close();

        // Handle case where no candidates are found
        if (empty($candidates)) {
            return [];
        }

        // Rerank candidates using cosine similarity (dot product of normalized vectors)
        $placeholders = implode(',', array_fill(0, count($candidates), '?'));
        $sql = "
        SELECT id, normalized_vector, MV_DOT_PRODUCT(normalized_vector, ?) AS similarity
        FROM %s
        WHERE id IN ($placeholders)
        ORDER BY similarity DESC
        LIMIT $n";
        $sql = sprintf($sql, $tableName);

        $statement = $this->mysqli->prepare($sql);

        if(!$statement) {
            throw new \Exception("Failed to prepare dot product query: " . $this->mysqli->error);
        }

        $normalizedVector = json_encode($normalizedVector);

        $types = str_repeat('i', count($candidates));
        $statement->bind_param('s' . $types, $normalizedVector, ...$candidates);

        $statement->execute();

        $statement->bind_result($id, $nv, $sim);

        $results = [];
        while ($statement->fetch()) {
            $results[] = [
                'id' => $id,
                'normalized_vector' => json_decode($nv, true),
                'similarity' => $sim
            ];
        }

        $statement->close();

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
        $tableName = $this->getVectorTableName();
        $statement = $this->mysqli->prepare("DELETE FROM $tableName WHERE id = ?");
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