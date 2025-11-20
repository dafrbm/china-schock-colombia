# =============================================================================
# CIIU Harmonization and Multi-level Concordance Builder
# Goal: Convert all CIIU codes to Rev 4 and build concordances at 2/3/4 digit levels
# =============================================================================

library(tidyverse)
library(haven)
library(concordance)

# Set paths
base_dir <- "C:/Users/dafrb/Desktop/EAM_data/china-schock"
processed_dir <- file.path(base_dir, "processed")
concordance_dir <- file.path(base_dir, "raw_data/concordance")
output_dir <- file.path(base_dir, "output")

# =============================================================================
# PART 1: Analyze EAM CIIU codes by year
# =============================================================================

cat("\n=== PART 1: EAM CIIU Analysis ===\n")

# Load EAM panel
eam_panel <- read_dta(file.path(processed_dir, "panel_eam_1992_2023.dta"))

# Check CIIU availability by year
ciiu_by_year <- eam_panel %>%
  group_by(year) %>%
  summarise(
    n_obs = n(),
    has_ciiu_4d = sum(!is.na(ciiu) & nchar(ciiu) >= 4) / n() * 100,
    has_ciiu_3d = sum(!is.na(ciiu_3d)) / n() * 100,
    has_ciiu_2d = sum(!is.na(ciiu_2d)) / n() * 100,
    n_unique_4d = n_distinct(ciiu[!is.na(ciiu)]),
    n_unique_3d = n_distinct(ciiu_3d[!is.na(ciiu_3d)]),
    n_unique_2d = n_distinct(ciiu_2d[!is.na(ciiu_2d)])
  )

cat("\nCIIU coverage by year:\n")
print(as.data.frame(ciiu_by_year))

# Identify CIIU version changes
# CIIU Rev 2: ~1992-2000 (codes start with 3xxx for manufacturing)
# CIIU Rev 3: ~2001-2011
# CIIU Rev 4: 2012+ (codes 10xx-33xx for manufacturing)

cat("\n\nSample CIIU codes by period:\n")
for (period in list(c(1992, 1995), c(2000, 2005), c(2010, 2015), c(2020, 2023))) {
  sample_codes <- eam_panel %>%
    filter(year >= period[1], year <= period[2]) %>%
    pull(ciiu) %>%
    unique() %>%
    head(10)
  cat(sprintf("\nYears %d-%d: %s\n", period[1], period[2], paste(sample_codes, collapse = ", ")))
}

# =============================================================================
# PART 2: Check concordance package alternatives
# =============================================================================

cat("\n=== PART 2: Concordance Package Investigation ===\n")

# The package might need different input formats
# Let's try various approaches

test_hs <- "847130"  # Computers

cat("\nTrying different concordance approaches for HS", test_hs, ":\n")

# Try HS -> SITC -> ISIC path
sitc_result <- tryCatch({
  concord(test_hs, "HS5", "SITC4", dest.digit = 5)
}, error = function(e) paste("Error:", e$message))
cat("HS5 -> SITC4:", paste(sitc_result, collapse = ","), "\n")

# Try older HS revisions
for (hs_rev in c("HS0", "HS1", "HS2", "HS3", "HS4", "HS5")) {
  result <- tryCatch({
    r <- concord(test_hs, hs_rev, "ISIC4", dest.digit = 2)
    if (length(r) == 0 || all(is.na(r))) "NA" else paste(r, collapse = ",")
  }, error = function(e) "Error")
  cat(sprintf("%s -> ISIC4: %s\n", hs_rev, result))
}

# Try via NAICS
naics_result <- tryCatch({
  concord(test_hs, "HS5", "NAICS2017", dest.digit = 6)
}, error = function(e) paste("Error:", e$message))
cat("HS5 -> NAICS2017:", paste(naics_result, collapse = ","), "\n")

# If we get NAICS, try NAICS -> ISIC
if (length(naics_result) > 0 && !is.na(naics_result[1]) && !grepl("Error", naics_result[1])) {
  isic_from_naics <- tryCatch({
    concord(naics_result[1], "NAICS2017", "ISIC4", dest.digit = 4)
  }, error = function(e) paste("Error:", e$message))
  cat("NAICS2017 -> ISIC4:", paste(isic_from_naics, collapse = ","), "\n")
}

# =============================================================================
# PART 3: Build ISIC Rev 2 -> Rev 4 concordance
# =============================================================================

cat("\n=== PART 3: ISIC Version Concordance ===\n")

# Test ISIC Rev 2 -> Rev 4 (for older EAM data)
# ISIC Rev 2 manufacturing: 3xxx codes
test_isic2 <- c("3111", "3211", "3411", "3511", "3710")  # Sample Rev 2 codes

cat("\nISIC Rev 2 -> Rev 4 mappings:\n")
for (code in test_isic2) {
  # Try direct mapping
  result_4d <- tryCatch({
    r <- concord(code, "ISIC2", "ISIC4", dest.digit = 4)
    if (length(r) == 0 || all(is.na(r))) "NA" else paste(r, collapse = ",")
  }, error = function(e) "Error")

  result_2d <- tryCatch({
    r <- concord(code, "ISIC2", "ISIC4", dest.digit = 2)
    if (length(r) == 0 || all(is.na(r))) "NA" else paste(r, collapse = ",")
  }, error = function(e) "Error")

  cat(sprintf("ISIC2 %s -> ISIC4: %s (2d: %s)\n", code, result_4d, result_2d))
}

# Also test ISIC Rev 3 -> Rev 4
test_isic3 <- c("1511", "1711", "2101", "2411", "2710")  # Sample Rev 3 codes

cat("\nISIC Rev 3 -> Rev 4 mappings:\n")
for (code in test_isic3) {
  result_4d <- tryCatch({
    r <- concord(code, "ISIC3", "ISIC4", dest.digit = 4)
    if (length(r) == 0 || all(is.na(r))) "NA" else paste(r, collapse = ",")
  }, error = function(e) "Error")

  cat(sprintf("ISIC3 %s -> ISIC4: %s\n", code, result_4d))
}

# =============================================================================
# PART 4: Sample size analysis by disaggregation level
# =============================================================================

cat("\n=== PART 4: Sample Size Analysis ===\n")

# For TFP estimation, we need sufficient observations per industry
# Rule of thumb: minimum 100 obs for reliable estimation

sample_size_2d <- eam_panel %>%
  filter(!is.na(ciiu_2d), ciiu_2d >= 10, ciiu_2d <= 33) %>%
  group_by(ciiu_2d) %>%
  summarise(n_obs = n(), n_firms = n_distinct(firm_id)) %>%
  arrange(desc(n_obs))

sample_size_3d <- eam_panel %>%
  filter(!is.na(ciiu_3d), ciiu_3d >= 100, ciiu_3d <= 339) %>%
  group_by(ciiu_3d) %>%
  summarise(n_obs = n(), n_firms = n_distinct(firm_id)) %>%
  arrange(desc(n_obs))

cat("\n2-digit level summary:\n")
cat("Industries:", nrow(sample_size_2d), "\n")
cat("With >= 100 obs:", sum(sample_size_2d$n_obs >= 100), "\n")
cat("With >= 500 obs:", sum(sample_size_2d$n_obs >= 500), "\n")
cat("Total obs covered (>=100):", sum(sample_size_2d$n_obs[sample_size_2d$n_obs >= 100]), "\n")

cat("\n3-digit level summary:\n")
cat("Industries:", nrow(sample_size_3d), "\n")
cat("With >= 100 obs:", sum(sample_size_3d$n_obs >= 100), "\n")
cat("With >= 500 obs:", sum(sample_size_3d$n_obs >= 500), "\n")
cat("Total obs covered (>=100):", sum(sample_size_3d$n_obs[sample_size_3d$n_obs >= 100]), "\n")

# Distribution of sample sizes
cat("\n2-digit sample size distribution:\n")
print(summary(sample_size_2d$n_obs))

cat("\n3-digit sample size distribution:\n")
print(summary(sample_size_3d$n_obs))

# =============================================================================
# PART 5: Recommendations
# =============================================================================

cat("\n=== PART 5: Recommendations ===\n")

# Identify which level is optimal
pct_covered_2d <- sum(sample_size_2d$n_obs[sample_size_2d$n_obs >= 100]) /
                  sum(sample_size_2d$n_obs) * 100
pct_covered_3d <- sum(sample_size_3d$n_obs[sample_size_3d$n_obs >= 100]) /
                  sum(sample_size_3d$n_obs) * 100

cat(sprintf("\nCoverage with >=100 obs threshold:
  2-digit: %.1f%% of observations
  3-digit: %.1f%% of observations
", pct_covered_2d, pct_covered_3d))

cat("
WORKFLOW RECOMMENDATIONS:

1. CIIU HARMONIZATION:
   - Identify CIIU version by year in EAM
   - Use concordance package for ISIC2->4 and ISIC3->4
   - Create harmonized ciiu_4d_harmonized variable

2. CONCORDANCE BUILDING:
   - Since HS->ISIC direct mapping has issues, keep using Pierce & Schott
   - Build at 3-digit and 4-digit levels in addition to current 2-digit

3. ANALYSIS STRATEGY:
   - Primary analysis: 2-digit (robust sample sizes)
   - Robustness check: 3-digit (more variation, smaller samples)
   - 4-digit likely too granular for TFP estimation

4. NEXT STEPS:
   a) Create CIIU harmonization table for EAM
   b) Rebuild HS->ISIC concordance at 3-digit
   c) Run parallel analyses at 2 and 3 digit levels
   d) Compare results for robustness
")

# Save analysis results
write_csv(sample_size_2d, file.path(output_dir, "sample_size_2digit.csv"))
write_csv(sample_size_3d, file.path(output_dir, "sample_size_3digit.csv"))
write_csv(ciiu_by_year, file.path(output_dir, "ciiu_coverage_by_year.csv"))

cat("\nAnalysis files saved to output/\n")
