set term postscript eps enhanced
set output "1k.eps"
set font "Helvetica"
set key right top
set xlabel "Runtime (sec)"
set ylabel "WAN throughput (Gbit/s)" 
set size 1.0, 1.0
set style line 1 lt 3 lw 1 
set style line 2 lt 1 lw 1
set style line 3 lt 2 lw 1
set style line 4 lt 7 lw 0.7 
set style line 5 lt 4 lw 1
set style line 6 lt 5 lw 1
set style line 7 lt 6 lw 1

set ytics nomirror
set y2tics nomirror
set y2tics ("real time" 0.8)	
set y2tics font "Helvetica,10"

plot [5:100][0:1.2] \
     "1k48-uva-vu.dat" using 1:3 title "1K-48 20G" w l ls 2, \
     "1k48-uva-vu-10G.dat" using 1:3 title "1K-48 10G" w l ls 1, \
     "1k48-uva-vu-internet.dat" using 1:3 title "1K-48 INT" w l ls 5, \
     0.8 title "" w l ls 4
