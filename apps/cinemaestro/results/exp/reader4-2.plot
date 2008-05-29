set term postscript eps enhanced
set output "reader-HD-24-30.eps"
set font "Helvetica"
set key right top
set xlabel "Runtime (sec)"
set ylabel "WAN throughput (Gbit/s)" 
set size 0.6,0.45
set style line 1 lt 3 lw 1 
set style line 2 lt 1 lw 1
set style line 3 lt 2 lw 1
set style line 4 lt 7 lw 1 
plot \
     "exp4-2.dat" using 1:3 title "Application" w l ls 2, \
     1.5 title "1.5 Gbit/s (realtime)" w l ls 3