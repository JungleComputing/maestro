set term postscript eps enhanced
set output "1k-24.eps"
set font "Helvetica"
set key right top
set xlabel "Runtime (sec)"
set ylabel "WAN throughput (Gbit/s)" 
set size 0.6, 0.45
set style line 1 lt 11 lw 1 
set style line 2 lt 1 lw 1
set style line 3 lt 2 lw 1
set style line 4 lt 7 lw 0.7 
set style line 5 lt 4 lw 1
set style line 6 lt 5 lw 1
set style line 7 lt 6 lw 1

set ytics nomirror
set y2tics nomirror
set y2tics ("real time" 0.37)	
set y2tics font "Helvetica,12"

plot [0:100][0:0.8] \
     "1k24-20G.dat" using 1:3 title "1K-24 20G" w l ls 2, \
     "1k24-10G.dat" using 1:3 title "1K-24 10G" w l ls 1, \
     "1k24-int.dat" using 1:3 title "1K-24 internet" w l ls 5
