<?php

namespace MHz\MysqlVector;

class VectorTable
{
    private string $name;
    private int $dimension;
    private string $engine;
    private \mysqli $mysqli;

    const SQL_COSIM_FUNCTION = "
CREATE FUNCTION COSIM(v1 JSON, v2 JSON) RETURNS FLOAT DETERMINISTIC BEGIN DECLARE sim FLOAT DEFAULT 0; DECLARE i INT DEFAULT 0; DECLARE len INT DEFAULT JSON_LENGTH(v1); IF JSON_LENGTH(v1) != JSON_LENGTH(v2) THEN RETURN NULL; END IF; WHILE i < len DO SET sim = sim + (JSON_EXTRACT(v1, CONCAT('$[', i, ']')) * JSON_EXTRACT(v2, CONCAT('$[', i, ']'))); SET i = i + 1; END WHILE; RETURN sim; END";

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

    protected function getCreateStatements(bool $ifNotExists = true): array {
        $binaryCodeLengthInBytes = ceil($this->dimension / 8);

        $vectorsQuery =
            "CREATE TABLE %s %s (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                vector JSON,
                normalized_vector JSON,
                magnitude DOUBLE,
                binary_code VARBINARY(%d),
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=%s;";
        $vectorsQuery = sprintf($vectorsQuery, $ifNotExists ? 'IF NOT EXISTS' : '', $this->getVectorTableName(), $binaryCodeLengthInBytes, $this->engine);

        return [$vectorsQuery];
    }

    /**
     * Convert an n-dimensional vector in to an n-bit binary code
     * @param array $vector
     * @return string
     */
    public function vectorToHex(array $vector): string {
        $bin = '';

        $bit = 0;
        $char = 0;
        foreach ($vector as $val){
            if ($val > 0){
                $char |= 1<<$bit;
            }

            $bit++;
            if ($bit === 8){
                $bin .= chr($char);
                $bit = 0;
                $char = 0;
            }
        }
        if ($bit > 0){
            $bin .= chr($char);
        }

        return bin2hex($bin);
    }

    /**
     * Create the tables required for storing vectors
     * @param bool $ifNotExists Whether to use IF NOT EXISTS in the CREATE TABLE statements
     * @return void
     * @throws \Exception If the tables could not be created
     */
    public function initialize(bool $ifNotExists = true): void
    {
        $this->mysqli->begin_transaction();
        foreach ($this->getCreateStatements($ifNotExists) as $statement) {
            $success = $this->mysqli->query($statement);
            if (!$success) {
                $e = new \Exception($this->mysqli->error);
                $this->mysqli->rollback();
                throw $e;
            }
        }

        // Add COSIM function
        $this->mysqli->query("DROP FUNCTION IF EXISTS COSIM");
        $res = $this->mysqli->query(self::SQL_COSIM_FUNCTION);

        if(!$res) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        // Drop the index if it exists.
        $tableName = $this->getVectorTableName();
        $query = "
        SELECT COUNT(1) index_exists
        FROM information_schema.statistics
        WHERE table_name=? AND index_name='idx_binary_code'
        ";
        $stmt = $this->mysqli->prepare($query);
        $stmt->bind_param('s', $tableName);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        if ($row['index_exists'] > 0) {
          $this->mysqli->query("DROP INDEX idx_binary_code ON " . $tableName);
        }
        $stmt->close();

        $binaryCodeLengthInBytes = ceil($this->dimension / 8);
        $this->mysqli->query("CREATE INDEX idx_binary_code ON " . $tableName . " (binary_code($binaryCodeLengthInBytes))");

        $this->mysqli->commit();
    }

    /**
     * Compute the cosine similarity between two normalized vectors
     * @param array $v1 The first vector
     * @param array $v2 The second vector
     * @return float The cosine similarity between the two vectors [0, 1]
     * @throws \Exception
     */
    public function cosim(array $v1, array $v2): float
    {
        $statement = $this->mysqli->prepare("SELECT COSIM(?, ?)");

        if(!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $v1 = json_encode($v1);
        $v2 = json_encode($v2);

        $statement->bind_param('ss', $v1, $v2);
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
        $magnitude = $this->getMagnitude($vector);
        $normalizedVector = $this->normalize($vector, $magnitude);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $tableName = $this->getVectorTableName();

        $insertQuery = empty($id) ?
            "INSERT INTO $tableName (vector, normalized_vector, magnitude, binary_code) VALUES (?, ?, ?, UNHEX(?))" :
            "UPDATE $tableName SET vector = ?, normalized_vector = ?, magnitude = ?, binary_code = UNHEX(?) WHERE id = $id";

        $statement = $this->mysqli->prepare($insertQuery);
        if(!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $vector = json_encode($vector);
        $normalizedVector = json_encode($normalizedVector);

        $statement->bind_param('ssds', $vector, $normalizedVector, $magnitude, $binaryCode);

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

        $statement = $this->getConnection()->prepare("INSERT INTO $tableName (vector, normalized_vector, magnitude, binary_code) VALUES (?, ?, ?, UNHEX(?))");
        if(!$statement) {
            throw new \Exception("Prepare failed: " . $this->getConnection()->error);
        }

        $ids = [];
        $this->getConnection()->begin_transaction();
        try {
            foreach ($vectorArray as $vector) {
                $magnitude = $this->getMagnitude($vector);
                $normalizedVector = $this->normalize($vector, $magnitude);
                $binaryCode = $this->vectorToHex($normalizedVector);
                $vectorJson = json_encode($vector);
                $normalizedVectorJson = json_encode($normalizedVector);

                $statement->bind_param('ssds', $vectorJson, $normalizedVectorJson, $magnitude, $binaryCode);

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
        $statement = $this->mysqli->prepare("SELECT id, vector, normalized_vector, magnitude, binary_code FROM $tableName WHERE id IN ($placeholders)");
        $types = str_repeat('i', count($ids));

        $refs = [];
        foreach ($ids as $key => $id) {
            $refs[$key] = &$ids[$key];
        }

        call_user_func_array([$statement, 'bind_param'], array_merge([$types], $refs));
        $statement->execute();
        $statement->bind_result($vectorId, $vector, $normalizedVector, $magnitude, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'vector' => json_decode($vector, true),
                'normalized_vector' => json_decode($normalizedVector, true),
                'magnitude' => $magnitude,
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    public function selectAll(): array {
        $tableName = $this->getVectorTableName();

        $statement = $this->mysqli->prepare("SELECT id, vector, normalized_vector, magnitude, binary_code FROM $tableName");

        if (!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $statement->execute();
        $statement->bind_result($vectorId, $vector, $normalizedVector, $magnitude, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'vector' => json_decode($vector, true),
                'normalized_vector' => json_decode($normalizedVector, true),
                'magnitude' => $magnitude,
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
     *               - 'magnitude': Vector magnitude
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

        // Rerank candidates using cosine similarity
        $placeholders = implode(',', array_fill(0, count($candidates), '?'));
        $sql = "
        SELECT id, vector, normalized_vector, magnitude, COSIM(normalized_vector, ?) AS similarity
        FROM %s
        WHERE id IN ($placeholders)
        ORDER BY similarity DESC
        LIMIT $n";
        $sql = sprintf($sql, $tableName);

        $statement = $this->mysqli->prepare($sql);

        if(!$statement) {
            $e = new \Exception($this->mysqli->error);
            $this->mysqli->rollback();
            throw $e;
        }

        $normalizedVector = json_encode($normalizedVector);

        $types = str_repeat('i', count($candidates));
        $statement->bind_param('s' . $types, $normalizedVector, ...$candidates);

        $statement->execute();

        $statement->bind_result($id, $v, $nv, $mag, $sim);

        $results = [];
        while ($statement->fetch()) {
            $results[] = [
                'id' => $id,
                'vector' => json_decode($v, true),
                'normalized_vector' => json_decode($nv, true),
                'magnitude' => $mag,
                'similarity' => $sim
            ];
        }

        $statement->close();

        return $results;
    }

    /**
     * Normalize a vector
     * @param array $vector The vector to normalize
     * @param float|null $magnitude The magnitude of the vector. If not provided, it will be calculated.
     * @param float $epsilon The epsilon value to use for normalization
     * @return array The normalized vector
     */
    private function normalize(array $vector, ?float $magnitude = null, float $epsilon = 1e-10): array {
        $magnitude = !empty($magnitude) ? $magnitude : $this->getMagnitude($vector);
        if ($magnitude == 0) {
            $magnitude = $epsilon;
        }
        foreach ($vector as $key => $value) {
            $vector[$key] = $value / $magnitude;
        }
        return $vector;
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