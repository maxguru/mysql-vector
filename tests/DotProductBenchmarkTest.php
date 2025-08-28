<?php

namespace MHz\MysqlVector\Tests;

/**
 * DotProductBenchmarkTest
 *
 * Purpose: Provide a test-driven, developer-runnable micro-benchmark to evaluate
 * different loop-unrolling strategies for dot product in PHP. It runs under PHPUnit and
 * integrates correctness checks to ensure all implementations compute the same
 * result within numeric tolerances.
 */
class DotProductBenchmarkTest extends BaseVectorTest
{
    // Workload settings (edit to tune benchmark)
    private const DIMS = [384, 1536, 3072];
    private const PAIRS_PER_DIM = 8;
    private const ITERS_PER_IMPL = 200;

    // -------------------- Implementations --------------------

    private static function dot_for(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        for ($i = 0; $i < $n; $i++) {
            $sum += $a[$i] * $b[$i];
        }
        return $sum;
    }

    private static function dot_for_unrolled2(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 2);
        for (; $i < $limit; $i += 2) {
            $sum +=
                ($a[$i] * $b[$i])
              + ($a[$i + 1] * $b[$i + 1]);
        }
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_for_unrolled4(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 4);
        for (; $i < $limit; $i += 4) {
            $sum += ($a[$i] * $b[$i])
                  + ($a[$i + 1] * $b[$i + 1])
                  + ($a[$i + 2] * $b[$i + 2])
                  + ($a[$i + 3] * $b[$i + 3]);
        }
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_for_unrolled8(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 8);
        for (; $i < $limit; $i += 8) {
            $sum +=
                ($a[$i] * $b[$i])
              + ($a[$i + 1] * $b[$i + 1])
              + ($a[$i + 2] * $b[$i + 2])
              + ($a[$i + 3] * $b[$i + 3])
              + ($a[$i + 4] * $b[$i + 4])
              + ($a[$i + 5] * $b[$i + 5])
              + ($a[$i + 6] * $b[$i + 6])
              + ($a[$i + 7] * $b[$i + 7]);
        }
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_for_unrolled16(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 16);
        for (; $i < $limit; $i += 16) {
            $sum +=
                ($a[$i] * $b[$i])
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
              + ($a[$i + 15] * $b[$i + 15]);
        }
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_for_unrolled32(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 32);
        for (; $i < $limit; $i += 32) {
            $sum +=
                ($a[$i] * $b[$i])
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
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_for_unrolled64(array $a, array $b): float
    {
        $n = count($a);
        $sum = 0.0;
        $i = 0; $limit = $n - ($n % 64);
        for (; $i < $limit; $i += 64) {
            $sum +=
                ($a[$i] * $b[$i])
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
              + ($a[$i + 31] * $b[$i + 31])
              + ($a[$i + 32] * $b[$i + 32])
              + ($a[$i + 33] * $b[$i + 33])
              + ($a[$i + 34] * $b[$i + 34])
              + ($a[$i + 35] * $b[$i + 35])
              + ($a[$i + 36] * $b[$i + 36])
              + ($a[$i + 37] * $b[$i + 37])
              + ($a[$i + 38] * $b[$i + 38])
              + ($a[$i + 39] * $b[$i + 39])
              + ($a[$i + 40] * $b[$i + 40])
              + ($a[$i + 41] * $b[$i + 41])
              + ($a[$i + 42] * $b[$i + 42])
              + ($a[$i + 43] * $b[$i + 43])
              + ($a[$i + 44] * $b[$i + 44])
              + ($a[$i + 45] * $b[$i + 45])
              + ($a[$i + 46] * $b[$i + 46])
              + ($a[$i + 47] * $b[$i + 47])
              + ($a[$i + 48] * $b[$i + 48])
              + ($a[$i + 49] * $b[$i + 49])
              + ($a[$i + 50] * $b[$i + 50])
              + ($a[$i + 51] * $b[$i + 51])
              + ($a[$i + 52] * $b[$i + 52])
              + ($a[$i + 53] * $b[$i + 53])
              + ($a[$i + 54] * $b[$i + 54])
              + ($a[$i + 55] * $b[$i + 55])
              + ($a[$i + 56] * $b[$i + 56])
              + ($a[$i + 57] * $b[$i + 57])
              + ($a[$i + 58] * $b[$i + 58])
              + ($a[$i + 59] * $b[$i + 59])
              + ($a[$i + 60] * $b[$i + 60])
              + ($a[$i + 61] * $b[$i + 61])
              + ($a[$i + 62] * $b[$i + 62])
              + ($a[$i + 63] * $b[$i + 63]);
        }
        for (; $i < $n; $i++) { $sum += $a[$i] * $b[$i]; }
        return $sum;
    }

    private static function dot_map(array $a, array $b): float
    {
        return array_sum(array_map(static fn ($x, $y) => $x * $y, $a, $b));
    }

    // -------------------- Utilities --------------------

    private static function makePairs(int $dimension, int $pairs): array
    {
        $out = [];
        for ($p = 0; $p < $pairs; $p++) {
            $a = []; $b = [];
            for ($i = 0; $i < $dimension; $i++) {
                // Uniform [0,1)
                $a[$i] = mt_rand() / mt_getrandmax();
                $b[$i] = mt_rand() / mt_getrandmax();
            }
            $out[] = [$a, $b];
        }
        return $out;
    }

    private static function bench(callable $fn, array $pairs, int $iters): array
    {
        $start = hrtime(true);
        $acc = 0.0; // avoid dead-code elimination
        $pairCount = count($pairs);
        for ($k = 0; $k < $iters; $k++) {
            [$a, $b] = $pairs[$k % $pairCount];
            $acc += $fn($a, $b);
        }
        $elapsedMs = (hrtime(true) - $start) / 1e6;
        return [$elapsedMs, $acc];
    }

    // -------------------- Tests --------------------

    public function testDotProductImplementationsProduceSameResults(): void
    {
        // Smaller dimensions for correctness checks, keep it fast
        $dims = [16, 127, 384];
        $pairsPerDim = 4;
        $tol = 1e-9; // strict since all use double internally

        foreach ($dims as $dim) {
            $pairs = self::makePairs($dim, $pairsPerDim);
            foreach ($pairs as [$a, $b]) {
                $base = self::dot_for($a, $b);
                $this->assertEqualsWithDelta($base, self::dot_map($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled2($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled4($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled8($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled16($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled32($a, $b), $tol);
                $this->assertEqualsWithDelta($base, self::dot_for_unrolled64($a, $b), $tol);
            }
        }
    }

    public function testDotProductBenchmark(): void
    {
        echo "\n\nDot product benchmark (PHP ".PHP_VERSION.")\n";
        $jit = ini_get('opcache.jit');
        if ($jit !== false && $jit !== '') {
            echo "OPcache JIT: ".$jit."\n";
        }
        echo str_repeat('=', 48)."\n";

        $impls = [
            'for-loop'               => [self::class, 'dot_for'],
            'for-loop (unroll x2)'   => [self::class, 'dot_for_unrolled2'],
            'for-loop (unroll x4)'   => [self::class, 'dot_for_unrolled4'],
            'for-loop (unroll x8)'   => [self::class, 'dot_for_unrolled8'],
            'for-loop (unroll x16)'  => [self::class, 'dot_for_unrolled16'],
            'for-loop (unroll x32)'  => [self::class, 'dot_for_unrolled32'],
            'for-loop (unroll x64)'  => [self::class, 'dot_for_unrolled64'],
            'array_map+sum'          => [self::class, 'dot_map'],
        ];

        $sawPositiveTiming = false;
        foreach (self::DIMS as $dim) {
            echo "=== {$dim}D ===\n";
            $pairsData = self::makePairs($dim, self::PAIRS_PER_DIM);

            // Warm-up (discard result)
            self::bench($impls['for-loop'], $pairsData, 10);

            $bestName = null; $bestTime = PHP_FLOAT_MAX;
            foreach ($impls as $name => $fn) {
                [$elapsedMs, $acc] = self::bench($fn, $pairsData, self::ITERS_PER_IMPL);
                printf("%-22s %8.2f ms  (acc=%0.3f)\n", $name, $elapsedMs, $acc);
                if ($elapsedMs > 0) { $sawPositiveTiming = true; }
                if ($elapsedMs < $bestTime) { $bestTime = $elapsedMs; $bestName = $name; }
            }

            echo sprintf("-> Fastest for %dD: %s (%.2f ms)\n\n", $dim, $bestName, $bestTime);
        }

        // Avoid risky test (no assertions) by asserting we measured something
        $this->assertTrue($sawPositiveTiming, 'Benchmark reported zero time; check environment.');
    }
}
