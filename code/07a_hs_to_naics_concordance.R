# HS to NAICS Concordance using concordance package
# Called from Stata

library(concordance)
library(tidyverse)

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {

  input_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
  output_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
} else {
  input_path <- args[1]
  output_path <- args[2]
}

# Load HS6 codes from existing temp file (HS6 is string to preserve leading zeros)
hs6_naics_temp <- haven::read_dta(file.path(input_path, "hs6_naics_temp.dta"))
hs6_codes <- unique(hs6_naics_temp$hs6)

# Convert HS to NAICS (try multiple HS revisions)
convert_hs_to_naics <- function(hs_code) {
  # hs_code is already string with leading zeros
  hs_str <- as.character(hs_code)

  for (hs_rev in c("HS5", "HS4", "HS3", "HS2", "HS1", "HS0", "HS")) {
    result <- tryCatch({
      r <- concord(hs_str, hs_rev, "NAICS", dest.digit = 6)
      if (length(r) > 0 && !all(is.na(r))) {
        return(list(naics = r[1], hs_revision = hs_rev))
      }
      NULL
    }, error = function(e) NULL)

    if (!is.null(result)) return(result)
  }

  return(list(naics = NA_character_, hs_revision = NA_character_))
}

# Process all codes
results <- map_dfr(hs6_codes, function(hs) {
  conversion <- convert_hs_to_naics(hs)
  data.frame(
    hs6 = hs,
    naics2017 = conversion$naics,
    hs_revision_used = conversion$hs_revision,
    stringsAsFactors = FALSE
  )
})
  
# Direct HS6 -> ISIC4 conversion (alternative path)
convert_hs_to_isic4 <- function(hs_code) {
  hs_str <- as.character(hs_code)
    
  for (hs_rev in c("HS5", "HS4", "HS3", "HS2", "HS1", "HS0", "HS")) {
    result <- tryCatch({
      r <- concord(hs_str, hs_rev, "ISIC4", dest.digit = 4, all = TRUE)
      if (length(r) > 0 && !is.null(r[[1]]$match) && length(r[[1]]$match) > 0) {
        # Get best match (highest weight)
        best_idx <- which.max(r[[1]]$weight)
        return(list(
          isic4 = r[[1]]$match[best_idx],
          weight = r[[1]]$weight[best_idx],
          hs_revision = hs_rev
          ))
        }
        NULL
      }, error = function(e) NULL)
      
      if (!is.null(result)) return(result)
    }
    
    return(list(isic4 = NA_character_, weight = NA_real_, hs_revision = NA_character_))
  }

# Process direct HS6 -> ISIC4
results_isic4 <- map_dfr(hs6_codes, function(hs) {
  conversion <- convert_hs_to_isic4(hs)
  data.frame(
    hs6 = hs,
    isic4 = conversion$isic4,
    isic4_weight = conversion$weight,
    hs_revision_used = conversion$hs_revision,
    stringsAsFactors = FALSE
  )
})

# Save results
write.csv(results, file.path(output_path, "hs6_naics2017_concordance.csv"), row.names = FALSE)
haven::write_dta(results, file.path(output_path, "hs6_naics2017_concordance.dta"))
write.csv(results_isic4, file.path(output_path, "hs6_isic4_direct_concordance.csv"), row.names = FALSE)
haven::write_dta(results_isic4, file.path(output_path, "hs6_isic4_direct_concordance.dta"))
