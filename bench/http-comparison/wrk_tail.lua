-- Report deep latency percentiles from wrk's existing histogram.
--
-- This callback runs only after the measured window. It deliberately defines
-- no request() or response() hook, so the generated HTTP load remains on
-- wrk's built-in request path.
done = function(_, latency, _)
    io.write(string.format(
        "CIO_WRK_TAIL_US p99.9=%d p99.99=%d p99.999=%d max=%d\n",
        latency:percentile(99.9),
        latency:percentile(99.99),
        latency:percentile(99.999),
        latency.max))
end
