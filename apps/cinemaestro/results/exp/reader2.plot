set term postscript eps enhanced
set output "reader-HD-48-inf.eps"
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
     "exp2.dat" using 1:3 title "Application" w l ls 2, \
     3.0 title "3.0 Gbit/s (realtime)" w l ls 3
