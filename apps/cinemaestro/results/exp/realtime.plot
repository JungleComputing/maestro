set term postscript eps enhanced
set output "realtime.eps"
set font "Helvetica"
set key right top
set xlabel "Runtime (sec)"
set ylabel "WAN throughput (Gbit/s)" 
set size 1.0, 1.0
set style line 1 lt 3 lw 2 
set style line 2 lt 1 lw 2
set style line 3 lt 2 lw 2
set style line 4 lt 7 lw 2 
set style line 5 lt 4 lw 1
set style line 6 lt 5 lw 2
set style line 7 lt 6 lw 2

set ytics nomirror
set y2tics nomirror
set y2tics ("11.9" 11.9, "6.0" 6.0, "3.0" 3.0, "1.5" 1.5, "0.8" 0.8, "0.4" 0.4)	
set y2tics font "Helvetica,10"

plot [5:100][0:20] \
     "4k48-uva-vu.dat" using 1:3 title "4K-48" w l ls 2, \
     "4k24-uva-vu.dat" using 1:3 title "4K-24" w l ls 3, \
     "2k48-uva-vu.dat" using 1:3 title "2K-48" w l ls 1, \
     "2k24-uva-vu.dat" using 1:3 title "2K-24" w l ls 4, \
     "1k48-uva-vu.dat" using 1:3 title "1K-48" w l ls 6, \
     "1k24-uva-vu.dat" using 1:3 title "1K-24" w l ls 7, \
     11.9 title "" w l ls 5, \
     6.0 title "" w l ls 5, \
     3.0 title "" w l ls 5, \
     1.5 title "" w l ls 5, \
     0.8 title "" w l ls 5, \
     0.4 title "" w l ls 5
