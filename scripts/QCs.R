#
mobile |>
  filter(gid == 6) |>
  collect() |>
  arrange(vid, date, t1) |>
  group_by(vid) |>
  mutate(x = case_when(is.na(t1) & is.na(t2) ~ "na_na",
                       is.na(t1) & !is.na(t2) ~ "na_ok",
                       !is.na(t1) & is.na(t2) ~ "ok_na",
                       lead(t1) == t2 ~ "same",
                       lead(t1) < t2 ~ "oops",
                       .default = "ok")) |>
  ungroup() |>
  mutate(year = year(date)) |>
  filter(year >= 2009) |>
  count(x) |>
  mutate(p = n / sum(n))
  count(year, x) |>
  ggplot(aes(year, n, fill = x)) +
  geom_col() +
  scale_fill_brewer(palette = "Set1")
