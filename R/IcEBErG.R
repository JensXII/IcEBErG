#' All hospitalizations and hospitalizations with respiratory diagnosis.
#'
#' Demand ODBC access to IB_EpiLPR
#'
#' @param start start season (year started).
#' @param end end season (year started).
#' @return Return season, agegrp, hospilitazions with respiratory diagnosis, all hospitalilazions, mean length of resp-hospitalitazions from season \code{start} to \code{end}.
#' @examples
#' \dontrun{
#' hosp(2010, 2017)
#' }
hosp <- function(start, end) {
  con <- RODBC::odbcConnect("HAIBA_TEST")
  hosp <- RODBC::sqlQuery(con, paste0("
                            select distinct b.cprnr, datediff(year, b.dob, b.course_in_date) as agegrp, b.course_in_date,
                            case when c.diagnosis is NULL then 0 else 1 end as resp,
                            datepart(year, b.course_in_date) as year, datepart(ISO_WEEK, b.course_in_date) as week,
                            datediff(hour, b.course_in_date, b.course_out_date) as ElengthHosp
                            from
                            IB_EpiLPR.EpiLPR3.data_course_admissions as b with(nolock)
                            left join
                            IB_EpiLPR.EpiLPR3.data_diagnoses as c with(nolock)
                            on (b.cprnr = c.cprnr) and (substring(c.diagnosis,1,2) = 'DJ') and
                            (b.course_in_date <= c.diagnosis_out_date) and (c.diagnosis_in_date <= b.course_out_date)
                            where (datediff(hour, b.course_in_date, b.course_out_date) >= 12) and
                            ('", start, "-07-01' <= b.course_in_date) and (b.course_in_date < '", end + 1, "-07-01') and
                            ((datepart(ISO_WEEK, b.course_in_date) >= 40) or (datepart(ISO_WEEK, b.course_in_date) <= 20))
                          "))
  RODBC::odbcClose(con)
  rm(con)
  hosp$cprnr <- as.character(hosp$cprnr)
  hosp$course_in_date <- as.Date(hosp$course_in_date)
  hosp$agegrp <- ifelse((2 <= hosp$agegrp) & (hosp$agegrp < 5), 2, pmin(5*ceiling(hosp$agegrp/5),75))
  hosp$hosp <- 1
  hosp$season <- hosp$year - as.numeric(hosp$week < 27)
  hosp$ElengthHosp <- hosp$Elength/24
  hosp$ElengthResp <- hosp$Elength
  hosp <- merge(
              merge(aggregate(cbind(resp, hosp) ~ season + agegrp, data = hosp, sum),
                    aggregate(ElengthResp ~ season + agegrp, data = subset(hosp, resp == 1), mean), by = c("season", "agegrp"), all = TRUE),
              aggregate(ElengthHosp ~ season + agegrp, data = hosp, mean), by = c("season", "agegrp"), all = TRUE)
  return(hosp)
}

#' Hospitalizations with a positive influenza sample during hospitalizaion.
#'
#' @param start start season (year started).
#' @param end end season (year started).
#' @return Return season, agegrp, hospilitazions with a positive influenza specimen, mean length of these hospitalizations from season \code{start} to \code{end}
#' @examples
#' \dontrun{
#' influ(2010, 2017)
#' }
influ <- function(start, end) {
  influ <- read.delim(paste0("S:/Data/Hosp and ICT/Data and output/", start, "/MiBa extract/influpos", start+1, "w39.csv"),
                      header = TRUE, sep = ",")
  names(influ) <- tolower(names(influ))
  influ <- influ[, c("cprnr", "prdate", "a", "b", "h1n1", "h3n2", "c")]
  for (year in (start+1):end) {
    d <- read.delim(paste0("S:/Data/Hosp and ICT/Data and output/", year, "/MiBa extract/influpos", year+1, "w39.csv"),
                    header = TRUE, sep = ",")
    names(d) <- tolower(names(d))
    influ <- rbind(influ, d[, c("cprnr", "prdate", "a", "b", "h1n1", "h3n2", "c")])
  }
  influ$cprnr <- as.character(influ$cprnr)
  influ$prdate <- as.Date(influ$prdate, "%d%b%y")
  influ <- unique(subset(influ, (a = 1) | (h1n1 = 1) | (h3n2 = 1) |(b = 1) | (c = 1))[, c("cprnr", "prdate")])
  con <- RODBC::odbcConnect("HAIBA_TEST")
  hosp <- RODBC::sqlQuery(con, paste0("
                            select cprnr, dob, course_in_date, course_out_date, datediff(hour, course_in_date, course_out_date) as ElengthInflu from
                            IB_EpiLPR.EpiLPR3.data_course_admissions with(nolock)
                            where (datediff(hour, course_in_date, course_out_date) >= 12) and
                            ('", start, "-07-01' <= course_in_date) and (course_in_date < '", end + 1, "-07-01') and
                            ((datepart(ISO_WEEK, course_in_date) >= 40) or (datepart(ISO_WEEK, course_in_date) <= 20))
                          "))
  RODBC::odbcClose(con)
  rm(con)
  hosp$cprnr <- as.character(hosp$cprnr)
  hosp$course_in_date <- as.Date(hosp$course_in_date)
  hosp$course_out_date <- as.Date(hosp$course_out_date)
  influ <- sqldf::sqldf("select distinct hosp.cprnr, hosp.dob, hosp.course_in_date, ElengthInflu from
                      hosp join influ
                      on (hosp.cprnr = influ.cprnr) and (hosp.course_in_date <= influ.prdate) and (influ.prdate <= course_out_date)
                    ")
  influ$agegrp <- as.numeric(floor((influ$course_in_date - influ$dob)/365.25))
  influ$agegrp <- pmax(0, ifelse((2 <= influ$agegrp) & (influ$agegrp < 5), 2, pmin(5*ceiling(influ$agegrp/5),75)))
  influ$year <- as.numeric(format(influ$course_in_date,"%Y"))
  influ$week <- as.numeric(substr(ISOweek::ISOweek(influ$course_in_date),7,8))
  influ$season <- influ$year - as.numeric(influ$week < 27)
  influ$influ <- 1
  influ$ElengthInflu <- influ$ElengthInflu/24
  influ <- merge(aggregate(influ ~ season + agegrp, data = influ, sum),
                 aggregate(ElengthInflu ~ season + agegrp, data = influ, mean), by = c("season", "agegrp"), all = TRUE)
  return(influ)
}

#' Population by age group by 1st January.
#'
#' @param start start year.
#' @param end end year.
#' @return Return season, agegrp, population by January 1 from season \code{start} to \code{end}.
#' @examples
#' \dontrun{
#' pop(2010, 2017)
#' }
pop <- function(start, end) {
  con <- RODBC::odbcConnect("DKMOMO")
  pop <- RODBC::sqlQuery(con, paste0("
                          select datepart(year, date) as season, agegrp, N from
                          EuroMOMO.DKMOMO.DKpopDateRegionSexAgegrp with(nolock)
                          where (datepart(day, date) = 1) and (datepart(month, date) = 1) and
                          (", start, " <= datepart(year, date)) and (datepart(year, date) <= ",end, ")
                          order by date, agegrp
                         "))
  RODBC::odbcClose(con)
  rm(con)
  pop$agegrp <- pmin(pop$agegrp, 75)
  pop <- aggregate(N ~ season + agegrp, data = pop, sum)
  return(pop)
}

#' Positive percentage among all specimens.
#'
#' @param start start season (year started).
#' @param end end season (year started).
#' @return Return season, agegrp, number of positive specimen and all specimens, and positive percentage from season \code{start} to \code{end}.
#' @examples
#' \dontrun{
#' PosPct(2010, 2017)
#' }
PosPct <- function(start, end) {
  influ <- read.delim(paste0("S:/Data/Hosp and ICT/Data and output/", start, "/MiBa extract/influpos", start+1, "w39.csv"),
                      header = TRUE, sep = ",")
  names(influ) <- tolower(names(influ))
  influ <- influ[!is.na(influ$prdate), c("cprnr", "prdate", "a", "b", "h1n1", "h3n2", "c")]
  for (year in (start+1):end) {
    d <- read.delim(paste0("S:/Data/Hosp and ICT/Data and output/", year, "/MiBa extract/influpos", year+1, "w39.csv"),
                    header = TRUE, sep = ",")
    names(d) <- tolower(names(d))
    influ <- rbind(influ, d[!is.na(influ$prdate), c("cprnr", "prdate", "a", "b", "h1n1", "h3n2", "c")])
  }
  influ$cprnr <- as.character(influ$cprnr)
  influ$prdate <- as.Date(influ$prdate, "%d%b%y")
  influ <- unique(subset(influ, !(is.na(a) & is.na(h1n1) & is.na(h3n2) & is.na(b) & is.na(c))))
  influ <- transform(influ, positive = as.numeric((a == 1) | (h1n1 == 1) | (h3n2 == 1) |(b == 1) | (c == 1)))[, c("cprnr", "prdate", "positive")]
  influ[is.na(influ$positive), "positive"] <- 0
  influ$year <- as.numeric(format(influ$prdate,"%Y"))
  influ$week <- as.numeric(substr(ISOweek::ISOweek(influ$prdate),7,8))
  influ <- subset(influ, (week >= 40) | (week <= 20))
  influ$season <- influ$year - as.numeric(influ$week < 27)
  influ$specimens <- 1
  influ <- aggregate(cbind(positive, specimens) ~ season, data = influ, sum)
  influ$PosPct <- 100 * influ$positive / influ$specimens
  return(influ)
}

#' Create ;-separated file with respiratory and influenza positive hospitalizations.
#'
#' @param start start season (year started).
#' @param end end season (year started).
#' @return Create ;-separated file with season, agegrp, number of positive specimen and all specimens, and positive percentage from season \code{start} to \code{end}.
#' @examples
#' \dontrun{
#' Writehosp(2010, 2017)
#' }
Writehosp <- function(start, end) {
  res <- merge(merge(hosp(start, end), pop(start, end), by = c("season", "agegrp"), all = TRUE),
             influ(start, end), by = c("season", "agegrp"), all = TRUE)
  res <- res[order(res$agegrp, res$season),]
  write.table(res, file = "S:/Data/Hosp and ICT/IcEBErG/InfluenzaHospitalizations.txt",
              row.names = FALSE, quote = FALSE, sep =";", dec=".", na="")
}

#' Create ;-separated file with Positive percentage among all specimens.
#'
#' @param start start season (year started).
#' @param end end season (year started).
#' @return Create ;-separated file with season, agegrp, number of positive specimen and all specimens, and positive percentage from season \code{start} to \code{end}.
#' @examples
#' \dontrun{
#' WritePosPct(2010, 2017)
#' }
WritePosPct <- function(start, end) {
  PosPct <- PosPct(start, end)
  write.table(PosPct, file = "S:/Data/Hosp and ICT/IcEBErG/PositivePercent.txt",
              row.names = FALSE, quote = FALSE, sep =";", dec=".", na="")
}
