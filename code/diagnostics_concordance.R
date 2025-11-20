# =============================================================================
# Diagnostics for HS6 -> CIIU Concordance
# Using R 'concordance' package (github.com/insongkim/concordance)
# =============================================================================

# Install concordance package if needed
# devtools::install_github("insongkim/concordance")

library(tidyverse)
library(haven)
library(concordance)

# Set paths
base_dir <- "C:/Users/dafrb/Desktop/EAM_data/china-schock"
processed_dir <- file.path(base_dir, "processed")
concordance_dir <- file.path(base_dir, "raw_data/concordance")
output_dir <- file.path(base_dir, "output")

# =============================================================================
# DIAGNOSTIC 1: Check concordance package mappings vs our concordance
# =============================================================================

# Load our concordance
our_concordance <- read_dta(file.path(concordance_dir, "hs6_ciiu2_concordance_detailed.dta"))

cat("\n=== Our Concordance Summary ===\n")
cat("Total HS6-CIIU pairs:", nrow(our_concordance), "\n")
cat("Unique HS6 codes:", n_distinct(our_concordance$hs6), "\n")
cat("CIIU 2-digit range:", range(our_concordance$ciiu_2d), "\n")

# Test concordance package - get available source/destination codes
cat("\n=== Concordance Package Available Mappings ===\n")

# Check HS to ISIC mapping using concordance package
# The package supports: HS0, HS1, HS2, HS3, HS4, HS5 (different HS revisions)
# and ISIC2, ISIC3, ISIC3.1, ISIC4

# Sample HS6 codes from our data
sample_hs6 <- head(unique(our_concordance$hs6), 20)

# Try to get ISIC4 codes using concordance package
cat("\nTesting concordance package mappings for sample HS6 codes:\n")

tryCatch({
  # Get HS to ISIC concordance from package
  # Note: Need to match HS revision year with data

  for (hs_code in sample_hs6) {
    hs_str <- sprintf("%06d", hs_code)

    # Try different HS revisions
    result <- tryCatch({
      concord(hs_str, origin = "HS5", destination = "ISIC4", dest.digit = 2)
    }, error = function(e) NA)

    our_ciiu <- our_concordance %>%
      filter(hs6 == hs_code) %>%
      pull(ciiu_2d) %>%
      unique()

    pkg_result <- if (length(result) == 0 || all(is.na(result))) "NA" else paste(result, collapse=",")
    cat(sprintf("HS6 %s: Package=%s, Ours=%s\n",
                hs_str,
                pkg_result,
                paste(our_ciiu, collapse=",")))
  }
}, error = function(e) {
  cat("Error using concordance package:", e$message, "\n")
})

# =============================================================================
# DIAGNOSTIC 2: Analyze match rates by year
# =============================================================================

cat("\n=== Match Rate Analysis ===\n")

# Load trade data to check match rates
trade_ciiu <- read_dta(file.path(processed_dir, "colombia_trade_ciiu.dta"))

# Check by year
match_by_year <- trade_ciiu %>%
  group_by(year) %>%
  summarise(
    n_industries = n(),
    total_china = sum(imports_china, na.rm = TRUE),
    total_world = sum(imports_total, na.rm = TRUE),
    china_share_calc = total_china / total_world,
    avg_share = mean(china_share, na.rm = TRUE)
  ) %>%
  arrange(year)

cat("\nChina Share by Year (checking calculation method):\n")
print(as.data.frame(match_by_year))

# Identify problematic years
problematic <- match_by_year %>%
  filter(china_share_calc > 1 | avg_share > 1)

if (nrow(problematic) > 0) {
  cat("\n!!! PROBLEMATIC YEARS (share > 100%):\n")
  print(as.data.frame(problematic))
}

# =============================================================================
# DIAGNOSTIC 3: Check china_share calculation issue
# =============================================================================

cat("\n=== China Share Calculation Check ===\n")

# The issue in the CSV is that china_share is calculated as MEAN across industries
# but this doesn't make sense - it should be total_china / total_world

timeseries_check <- trade_ciiu %>%
  group_by(year) %>%
  summarise(
    # Correct way: sum first, then divide
    correct_share = sum(imports_china) / sum(imports_total),
    # What was done: mean of shares (incorrect for aggregation)
    mean_of_shares = mean(china_share),
    # Difference
    diff = correct_share - mean_of_shares
  )

cat("\nComparing calculation methods:\n")
print(as.data.frame(timeseries_check %>% filter(year >= 2015)))

# =============================================================================
# DIAGNOSTIC 4: Industry-level anomalies
# =============================================================================

cat("\n=== Industry-Level Anomalies ===\n")

# Find industries with china_share > 1 (impossible)
anomalies <- trade_ciiu %>%
  filter(china_share > 1) %>%
  arrange(desc(china_share))

if (nrow(anomalies) > 0) {
  cat("\n!!! Found", nrow(anomalies), "observations with china_share > 100%:\n")
  print(as.data.frame(head(anomalies, 20)))
}

# =============================================================================
# DIAGNOSTIC 5: Coverage analysis
# =============================================================================

cat("\n=== Coverage Analysis ===\n")

# Check which CIIU industries are covered
covered_ciiu <- sort(unique(trade_ciiu$ciiu_2d))
# Note: Stata filters 10-39 but strict manufacturing is 10-33
# ISIC4: 10-33=Manufacturing, 35=Electricity, 36-39=Water/Waste
expected_manufacturing <- 10:33
expected_stata_filter <- 10:39

missing_manufacturing <- setdiff(expected_manufacturing, covered_ciiu)
extra_non_manufacturing <- setdiff(covered_ciiu, expected_manufacturing)

cat("Covered CIIU codes:", paste(covered_ciiu, collapse = ", "), "\n")
cat("Missing manufacturing (10-33):", paste(missing_manufacturing, collapse = ", "), "\n")
cat("Non-manufacturing codes present:", paste(extra_non_manufacturing, collapse = ", "), "\n")

# =============================================================================
# DIAGNOSTIC 5b: Check for data inconsistency causing share > 1
# =============================================================================

cat("\n=== Data Inconsistency Check ===\n")

# Find observations where imports_china > imports_total (impossible)
impossible_obs <- trade_ciiu %>%
  filter(imports_china > imports_total) %>%
  mutate(excess = imports_china - imports_total) %>%
  arrange(desc(excess))

if (nrow(impossible_obs) > 0) {
  cat("\n!!! Found", nrow(impossible_obs), "obs where China imports > Total imports:\n")
  print(as.data.frame(head(impossible_obs, 15)))

  # Summary by year
  cat("\nBy year:\n")
  impossible_by_year <- impossible_obs %>%
    group_by(year) %>%
    summarise(
      n_industries = n(),
      total_excess = sum(excess)
    )
  print(as.data.frame(impossible_by_year))
}

# =============================================================================
# DIAGNOSTIC 6: Export diagnostic report
# =============================================================================

# Create diagnostic summary
diagnostic_summary <- list(
  our_concordance_stats = list(
    n_pairs = nrow(our_concordance),
    n_hs6 = n_distinct(our_concordance$hs6),
    ciiu_range = range(our_concordance$ciiu_2d)
  ),
  problematic_years = problematic,
  anomaly_count = nrow(anomalies),
  impossible_obs_count = nrow(impossible_obs),
  coverage = list(
    covered = covered_ciiu,
    missing_manufacturing = missing_manufacturing,
    non_manufacturing = extra_non_manufacturing
  )
)

# Save detailed anomalies
if (nrow(anomalies) > 0) {
  write_csv(anomalies, file.path(output_dir, "diagnostic_anomalies.csv"))
  cat("\nAnomaly details saved to: output/diagnostic_anomalies.csv\n")
}

# Save timeseries check
write_csv(timeseries_check, file.path(output_dir, "diagnostic_timeseries_check.csv"))
cat("Timeseries check saved to: output/diagnostic_timeseries_check.csv\n")

cat("\n=== Diagnostics Complete ===\n")
