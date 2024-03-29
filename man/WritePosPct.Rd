% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/IcEBErG.R
\name{WritePosPct}
\alias{WritePosPct}
\title{Create ;-separated file with Positive percentage among all specimens.}
\usage{
WritePosPct(start, end)
}
\arguments{
\item{start}{start season (year started).}

\item{end}{end season (year started).}
}
\value{
Create ;-separated file with season, agegrp, number of positive specimen and all specimens, and positive percentage from season \code{start} to \code{end}.
}
\description{
Create ;-separated file with Positive percentage among all specimens.
}
\examples{
\dontrun{
WritePosPct(2010, 2017)
}
}
