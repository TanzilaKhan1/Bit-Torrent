    AREA |.data|, DATA, READWRITE
DataBytes   DCB  0xFF, 0x80, 0x75, 0x90   ; Four bytes that will generate carry
Result      DCB  0                        ; Store final 8-bit sum
CarryFlag   DCB  0                        ; Store carry count


    AREA |.text|, CODE, READONLY
    ENTRY
    EXPORT main

main
    LDR R4, =DataBytes       ; R4 = base address of data
    MOV R1, #0               ; R1 = byte index
    MOV R2, #0               ; R2 = running sum (8-bit)
    MOV R5, #0               ; R5 = carry counter

loop_start
    CMP R1, #4               
    BGE done

    LDRB R3, [R4, R1]        ; Load byte at offset R1 into R3
    BL Add_byte              ; Call Add_byte (R2: sum, R3: byte)

    ADD R1, R1, #1           
    B loop_start

done
    LDR R0, =Result
    STRB R2, [R0]            ; Store final 8-bit result

    LDR R0, =CarryFlag
    STRB R5, [R0]            ; Store carry count

    B stop

stop
    B stop


; -----------------------------------------------------------
; Function: Add_byte
; Inputs:
;   R2 = running sum (8-bit)
;   R3 = byte to add
; Outputs:
;   R2 = updated 8-bit sum
;   R5 = carry counter incremented if carry occurs
; Clobbers: R4, R6
; -----------------------------------------------------------

Add_byte
    ; Clear upper bits to do clean 8-bit operation
    AND R2, R2, #0xFF        
    AND R3, R3, #0xFF        
    
    ADDS R6, R2, R3          ; R6 = R2 + R3, sets flags
    
    ; Check if result exceeds 8-bit range by testing bit 8
    TST R6, #0x100           ; Test bit 8 (256)
    BEQ no_carry_method2     ; If bit 8 is clear, no 8-bit overflow
     
    ADD R5, R5, #1           ; carry occurred
    
no_carry_method2
    AND R2, R6, #0xFF        ; Mask to 8-bit result
    BX LR

    END
