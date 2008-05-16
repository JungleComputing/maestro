set term postscript eps enhanced
set output "1k.eps"
set font "Helvetica"
set key right top
set xlabel "Runtime (sec)"
set ylabel "WAN throughput (Gbit/s)" 
set size 1.0, 1.0
set style line 1 lt 3 lw 1 
set style line 2 lt 1 lw 0.5
set style line 3 lt 2 lw 1
set style line 4 lt 7 lw 0.7 
set style line 5 lt 4 lw 1
set style line 6 lt 5 lw 1
set style line 7 lt 6 lw 1

set ytics nomirror
set y2tics nomirror
set y2tics ("real time" 0.8)	
set y2tics font "Helvetica,10"

plot \
     "exp14.out.0.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.1.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.2.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.3.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.4.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.5.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.6.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.7.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.8.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.9.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.10.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.11.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.12.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.13.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.14.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.15.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.16.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.17.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.18.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.19.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.20.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.21.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.22.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.23.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.24.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.25.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.26.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.27.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.28.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.29.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.30.dat" using 1:3 title "" w l ls 2, \
     "exp14.out.31.dat" using 1:3 title "" w l ls 2

