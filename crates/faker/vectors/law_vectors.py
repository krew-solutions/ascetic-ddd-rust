"""Reference values of ascetic-ddd-faker's laws, computed outside the crate (ADR-0045, L1).

mpmath at 50 significant digits evaluates each quantile at the exact
probability a unit stands for, p = (k + 1/2) / 2^52, and each special
function at a few points. The crate's own implementations (pinned libm,
continued fractions, a walk from the mode) are checked against these.

Run: python3 -m venv venv && venv/bin/pip install mpmath==1.4.1 &&
     venv/bin/python -u crates/faker/vectors/law_vectors.py
"""

import mpmath as mp

mp.mp.dps = 50
UNITS = 2**52

# Units across the range: both extremes, the tails, the middle.
KS = [0, 1, 4503599627, UNITS // 1000, UNITS // 10, UNITS // 2, UNITS - UNITS // 10,
      UNITS - UNITS // 1000, UNITS - 4503599628, UNITS - 2, UNITS - 1]


def p_of(k):
    return (mp.mpf(k) + mp.mpf(1) / 2) / UNITS


def f(x):
    """The double nearest the exact value, written so that it reads back as itself."""
    return repr(float(x))


def continuous(name, quantile):
    for k in KS:
        print(f'    ("{name}", {k}, {f(quantile(p_of(k)))}),')


def least(cdf, p, lo, hi):
    """The least integer x in [lo, hi] with cdf(x) >= p."""
    while lo < hi:
        mid = (lo + hi) // 2
        if cdf(mid) >= p:
            hi = mid
        else:
            lo = mid + 1
    return lo


def poisson_cdf(lam):
    return lambda x: mp.gammainc(x + 1, lam, mp.inf, regularized=True)


def binomial_table(n, q):
    """P(X <= x) for x over mean +- 40 sigma, from the masses at 50 digits: the
    mass at the window's start by log-gamma, the rest by the ratio of
    neighbours, the mass below the window by the incomplete beta function."""
    mean, sigma = n * q, mp.sqrt(n * q * (1 - q))
    lo, hi = max(0, int(mean - 40 * sigma)), min(n, int(mean + 40 * sigma) + 1)
    mass = mp.exp(mp.loggamma(n + 1) - mp.loggamma(lo + 1) - mp.loggamma(n - lo + 1)
                  + lo * mp.log(q) + (n - lo) * mp.log(1 - q))
    below = mp.mpf(0) if lo == 0 else mp.betainc(n - lo + 1, lo, 0, 1 - q, regularized=True)
    table, total = [], below
    for x in range(lo, hi + 1):
        total += mass
        table.append(total)
        mass = mass * (n - x) / (x + 1) * q / (1 - q)
    return lo, table


def binomial_quantile(lo, table, p):
    left, right = 0, len(table) - 1
    while left < right:
        mid = (left + right) // 2
        if table[mid] >= p:
            right = mid
        else:
            left = mid + 1
    return lo + left


def main():
    print("// continuous: (law, k, quantile)")
    continuous("exponential(rate=2.5)", lambda p: -mp.log(1 - p) / mp.mpf("2.5"))
    continuous("pareto(scale=1, shape=1.16)", lambda p: (1 - p) ** (-1 / mp.mpf("1.16")))
    continuous("pareto(scale=3, shape=2.5)", lambda p: 3 * (1 - p) ** (-1 / mp.mpf("2.5")))
    continuous("lognormal(mu=0, sigma=1)", lambda p: mp.exp(mp.sqrt(2) * mp.erfinv(2 * p - 1)))
    continuous("lognormal(mu=2, sigma=0.5)",
               lambda p: mp.exp(2 + mp.mpf("0.5") * mp.sqrt(2) * mp.erfinv(2 * p - 1)))
    continuous("weibull(scale=1, shape=1.5)", lambda p: (-mp.log(1 - p)) ** (1 / mp.mpf("1.5")))
    continuous("weibull(scale=2, shape=0.5)", lambda p: 2 * (-mp.log(1 - p)) ** 2)
    continuous("normal", lambda p: mp.sqrt(2) * mp.erfinv(2 * p - 1))

    print("// discrete: (law, k, quantile)")
    for lam in ["0.5", "30", "1000", "100000"]:
        cdf = poisson_cdf(mp.mpf(lam))
        sigma = int(mp.sqrt(mp.mpf(lam))) + 1
        for k in KS:
            x = least(cdf, p_of(k), max(0, int(mp.mpf(lam)) - 40 * sigma), int(mp.mpf(lam)) + 40 * sigma + 40)
            print(f'    ("poisson(mean={lam})", {k}, {x}),')
    for n, q in [(100, "0.3"), (10000, "0.5"), (1000000, "0.01")]:
        lo, table = binomial_table(n, mp.mpf(q))
        for k in KS:
            print(f'    ("binomial(trials={n}, p={q})", {k}, {binomial_quantile(lo, table, p_of(k))}),')

    print("// special functions: (name, a, b, x, value)")
    for a, x in [("0.5", "0.1"), ("1", "1"), ("5", "2"), ("30", "31"), ("1000", "990"), ("100001", "100000")]:
        v = mp.gammainc(mp.mpf(a), 0, mp.mpf(x), regularized=True)
        print(f'    ("gamma_p", {a}, 0.0, {x}, {f(v)}),')
        v = mp.gammainc(mp.mpf(a), mp.mpf(x), mp.inf, regularized=True)
        print(f'    ("gamma_q", {a}, 0.0, {x}, {f(v)}),')
    for a, b, x in [("1", "1", "0.3"), ("2", "5", "0.2"), ("70", "31", "0.7"), ("5000", "5001", "0.5")]:
        v = mp.betainc(mp.mpf(a), mp.mpf(b), 0, mp.mpf(x), regularized=True)
        print(f'    ("beta_i", {a}, {b}, {x}, {f(v)}),')


if __name__ == "__main__":
    main()
