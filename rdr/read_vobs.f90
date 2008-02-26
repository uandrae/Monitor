SUBROUTINE read_vobs

 USE data
 USE functions
 USE timing
 USE constants

 IMPLICIT NONE


 INTEGER :: i,ii,j,k,l,			&
            ierr = 0,			&
            cdate = 999999,		&
            ctime = 999999,		&
            wdate = 999999,		&
            wtime = 999999,		&
            istnr = 0,			&
            stat_i,				&
            num_temp,num_stat,	&
            num_temp_lev,		&
            stations(100000),	&
            max_found_stat,		&
            timing_id,          &
            version_flag
            
 
 
 REAL :: lat,lon,hgt,val(10)

 CHARACTER(LEN=100) :: fname =' '
 CHARACTER(LEN= 10) :: ndate =' '

 LOGICAL :: qco,use_stnlist

!----------------------------------------------------------

 stations       = 0
 max_found_stat = 0

 use_stnlist =(  MAXVAL(stnlist) > 0 )

 CALL allocate_obs

 ! Copy time

 cdate = sdate
 ctime = stime
 wdate = cdate
 wtime = ctime
 
 !
 ! Loop over all times
 !

 i = 0

 TIME_LOOP : DO

 IF (print_read > 1) WRITE(6,*)'TIME:',cdate,ctime/10000
 WRITE(ndate(1:10),'(I8.8,I2.2)')cdate,ctime/10000
 fname = TRIM(obspath)//'vobs'//ndate

 i = i + 1

 !
 ! Read obs data
 !

       OPEN(lunin,file=fname,status='old',iostat=ierr)

       IF (ierr /= 0) THEN
  
          IF( print_read > 0 )WRITE(6,*)'Could not open:',TRIM(fname)

          wdate = cdate
          wtime = ctime
          CALL adddtg(wdate,wtime,3600*obint,cdate,ctime)
          IF(cdate.gt.edate_obs) EXIT TIME_LOOP

          i = i - 1
          CYCLE TIME_LOOP

       ENDIF

       IF (print_read > 0 ) WRITE(6,*)'READ ',TRIM(fname)

       version_flag = 0

       READ(lunin,'(1x,3I6)',IOSTAT=ierr)num_stat,num_temp,version_flag
       IF ( ierr /= 0 ) THEN
          WRITE(6,*)'Error reading firrst line of vobs file',ierr
          CALL abort
       ENDIF
      
       READ(lunin,*)num_temp_lev

       READ_STATION_OBS : DO k=1,num_stat

          val = -99.
          SELECT CASE(version_flag)
 
          CASE(0)
             READ(lunin,*,iostat=ierr)istnr,lat,lon,hgt,val(1:8)
          CASE(1)
             READ(lunin,*,iostat=ierr)istnr,lat,lon,hgt,val(1:10)
          CASE DEFAULT
             WRITE(6,*)'Cannot handle this vobs-file version',version_flag
             CALL abort

          END SELECT

          IF (ierr  /= 0) CYCLE READ_STATION_OBS
          IF (istnr == 0) CYCLE READ_STATION_OBS

          !
          ! Find station index
          !

          IF(stations(istnr) == 0) THEN
           
             stat_i = 0
             IF ( use_stnlist ) THEN
                DO ii=1,maxstn
                   IF (istnr == stnlist(ii) ) stat_i = ii
                ENDDO
                IF ( stat_i == 0 ) CYCLE READ_STATION_OBS
             ENDIF

             IF (stat_i == 0 ) THEN 
                max_found_stat  = max_found_stat + 1
                stnlist(max_found_stat)= istnr
             ELSE
                max_found_stat  = stat_i
             ENDIF

             stations(istnr) = max_found_stat 
             obs(max_found_stat)%active = .TRUE.
             obs(max_found_stat)%stnr   = istnr
             obs(max_found_stat)%lat    = lat
             obs(max_found_stat)%lon    = lon
             obs(max_found_stat)%hgt    = hgt

             IF (max_found_stat > maxstn) THEN
                WRITE(6,*)'Increase maxstn',max_found_stat
                CALL abort
             ENDIF

          ENDIF

          stat_i = stations(istnr)

          !
          ! Make Celcius
          !

          i = obs(stat_i)%ntim + 1

          IF ( print_read > 1 ) WRITE(6,*)'STATION ',stat_i,obs(stat_i)%stnr

          ALLOCATE(obs(stat_i)%o(i)%date)
          ALLOCATE(obs(stat_i)%o(i)%time)
          ALLOCATE(obs(stat_i)%o(i)%val(nparver))

          obs(stat_i)%ntim      = i
          obs(stat_i)%o(i)%date = cdate
          obs(stat_i)%o(i)%time = ctime/10000
          IF ( use_pos ) obs(stat_i)%pos(cdate * 100 + ctime/10000 ) = i
          obs(stat_i)%o(i)%val  = err_ind

          if (nn_ind /= 0 .AND. qco(val(1)).AND.qcl(val(1),nn_ind).AND. &
                                qcu(val(1),nn_ind)) obs(stat_i)%o(i)%val(nn_ind) = val(1)
          if (dd_ind /= 0 .AND. qco(val(2)).AND.qcl(val(2),dd_ind).AND. &
                                qcu(val(2),dd_ind)) obs(stat_i)%o(i)%val(dd_ind) = val(2)
          if (ff_ind /= 0 .AND. qco(val(3)).AND.qcl(val(3),ff_ind).AND. &
                                qcu(val(3),ff_ind)) obs(stat_i)%o(i)%val(ff_ind) = val(3)
          if (tt_ind /= 0 .AND. qco(val(4)).AND.qcl(val(4)-tzero,tt_ind).AND. &
                                qcu(val(4)-tzero,tt_ind)) obs(stat_i)%o(i)%val(tt_ind) = val(4) - tzero
          if (rh_ind /= 0 .AND. qco(val(5)).AND.qcl(val(5),rh_ind).AND. &
                                qcu(val(5),rh_ind)) obs(stat_i)%o(i)%val(rh_ind) = val(5)
          if (ps_ind /= 0 .AND. qco(val(6)).AND.qcl(val(6),ps_ind).AND. &
                                qcu(val(6),ps_ind)) obs(stat_i)%o(i)%val(ps_ind) = val(6)
          if (pe_ind /= 0 .AND. qco(val(7)).AND.qcl(val(7),pe_ind).AND. &
                                qcu(val(7),pe_ind)) obs(stat_i)%o(i)%val(pe_ind) = val(7)
          if (qq_ind /= 0 .AND. qco(val(8)).AND.qcl(val(8),qq_ind).AND. &
                                qcu(val(8),qq_ind)) obs(stat_i)%o(i)%val(qq_ind) = val(8) * 1.e3
          if (vi_ind /= 0 .AND. qco(val(9)).AND.qcl(val(9),vi_ind).AND. &
                                qcu(val(9),vi_ind)) obs(stat_i)%o(i)%val(vi_ind) = val(9) 
          if (td_ind /= 0 .AND. qco(val(10)).AND.qcl(val(10),td_ind).AND. &
                                qcu(val(10),td_ind)) obs(stat_i)%o(i)%val(td_ind) = val(10) 

          if (la_ind /= 0 ) obs(stat_i)%o(i)%val(la_ind) = obs(stat_i)%lat
          if (hg_ind /= 0 ) obs(stat_i)%o(i)%val(hg_ind) = obs(stat_i)%hgt

          IF (print_read > 1 ) WRITE(6,*)obs(stat_i)%o(i)%val

       ENDDO READ_STATION_OBS

       CLOSE(lunin)

    wdate = cdate
    wtime = ctime
    CALL adddtg(wdate,wtime,3600*obint,cdate,ctime)
    IF(cdate.gt.edate_obs) EXIT TIME_LOOP

 ENDDO TIME_LOOP

 WRITE(6,*) 'FOUND TIMES OBS',obs(1)%ntim

 DO i=1,maxstn
    obs(i)%active = ( obs(i)%ntim > 0 )
 ENDDO

 RETURN

END SUBROUTINE read_vobs
LOGICAL FUNCTION qco(a)

USE DATA, only : err_ind

IMPLICIT NONE

REAL    :: a

 qco = (ABS(a+99.) > 1.e-6 )

END FUNCTION qco
